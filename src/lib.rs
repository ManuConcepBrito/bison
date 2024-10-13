use lru::LruCache;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::PyErr;
use pyo3::PyObject;
use pythonize::{depythonize, pythonize};
use query::{QueryOperator, UpdateOperator};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::fs;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::io::BufWriter;
use std::io::{BufReader, Write};
use std::path::PathBuf;

mod query;

#[pyclass]
pub struct Bison {
    base_path: PathBuf,
    collections: Map<String, Value>,
    query_cache: LruCache<u64, Vec<Value>>,
}
impl Bison {
    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(collection_name);
        path.set_extension("json");
        path
    }

    fn read_document(document_name: &str) -> Result<Value, PyErr> {
        let file_path = PathBuf::from(document_name);
        let file_result = OpenOptions::new().read(true).open(&file_path);

        let file = match file_result {
            Ok(file) => file,
            Err(err) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Error opening document {}",
                    err
                )));
            }
        };
        let reader = BufReader::new(file);

        // Parse the file into a serde_json::Value
        let json_value: Value = serde_json::from_reader(reader)
            .map_err(|_| PyErr::new::<PyValueError, _>("Error deserializing JSON"))?;
        Ok(json_value)
    }
    fn find_collection_in_storage(&mut self, collection_name: &str) -> Result<Vec<Value>, PyErr> {
        // Try to load from disk
        let mut collection_path = self.base_path.clone().join(PathBuf::from(collection_name));
        collection_path.set_extension("json");
        if !collection_path.exists() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Collection with name '{}' not found on disk",
                collection_name
            )));
        }

        // Load the collection from disk
        let values_in_storage: Map<String, Value> =
            Bison::read_document(collection_path.to_str().unwrap())?
                .as_object()
                .unwrap()
                .to_owned();

        // Extract the collection by key, convert to Vec<Value> and return as owned
        let loaded_collection = values_in_storage
            .get(collection_name)
            .unwrap()
            .as_array()
            .unwrap()
            .clone();

        // Store it in `self.collections` for future use
        self.collections.insert(
            collection_name.to_string(),
            Value::Array(loaded_collection.clone()),
        );

        Ok(loaded_collection)
    }

    pub fn extract_collection(
        json_value: Value,
        collection_name: String,
    ) -> Result<Vec<Value>, PyErr> {
        // Ensure the parsed JSON is an object and get a reference to it
        let collection = json_value
            .as_object()
            .ok_or_else(|| PyErr::new::<PyValueError, _>("Error in collection deserialization"))?;
        // Get the collection array from the object
        let collection_array = collection
            .get(&collection_name)
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                PyErr::new::<PyValueError, _>(
                    "Collection does not contain collection key or is not an array",
                )
            })?;
        Ok(collection_array.to_vec())
    }
    fn insert_in_collection(
        &mut self,
        collection_name: &str,
        insert_value: Value,
    ) -> Result<(), PyErr> {
        // Create collection if it does not exist
        if !self.collections.contains_key(collection_name) {
            let _ = self.create_collection(collection_name);
        }

        let collection = self.collections.get_mut(collection_name);

        if let Some(Value::Array(arr)) = collection {
            // Extend the collection if the value to insert is an array
            if let Some(insert_value_arr) = insert_value.as_array() {
                arr.extend_from_slice(insert_value_arr)
            } else {
                arr.push(insert_value);
            }
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Expected collection to be array",
            ));
        }
        Ok(())
    }

    fn _find(
        &mut self,
        collection_name: &str,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> Result<Vec<Value>, PyErr> {
        // Inner method that returns Vec<Value> instead
        // of a python dict
        let in_memory_collection = self.collections.get(collection_name);
        let collection_values: Cow<Vec<Value>> = match in_memory_collection {
            Some(c) => Cow::Borrowed(c.as_array().unwrap()),
            None => match self.find_collection_in_storage(collection_name) {
                Ok(in_storage_collection) => Cow::Owned(in_storage_collection),
                Err(err) => return Err(err),
            },
        };
        let query: Value = match maybe_query {
            Some(q) => depythonize(q).unwrap(),
            None => {
                // If there is no query, return all the values
                return Ok(collection_values.to_vec());
            }
        };

        let query_object: &Map<String, Value> = query.as_object().unwrap();
        let mut hasher = DefaultHasher::new();
        query_object.hash(&mut hasher);
        let query_hash = hasher.finish();
        if let Some(cached_collections) = self.query_cache.get(&query_hash) {
            return Ok(cached_collections.to_vec());
        }
        let query_engine = query::QueryEngine::<QueryOperator>::new(query_object);
        // execute queries and return collections
        let mut found_collections: Vec<Value> = vec![];
        for collection in collection_values.iter() {
            let c_obj = collection.as_object().unwrap();
            let query_result = query_engine.execute(c_obj);
            match query_result {
                Ok(result) => {
                    if result {
                        found_collections.push(collection.clone());
                    }
                }
                Err(err) => return Err(err),
            }
        }
        self.query_cache.put(query_hash, found_collections.to_vec());
        Ok(found_collections)
    }
    fn _update(
        &mut self,
        mut collection_values: Vec<Value>,
        py_update_query: &Bound<'_, PyDict>,
        maybe_filter_query: Option<&Bound<'_, PyDict>>,
    ) -> Result<Vec<Value>, PyErr> {
        let update_query: Value = depythonize(py_update_query).unwrap();
        let update_query_object: &Map<String, Value> = update_query.as_object().unwrap();
        let update_query_engine = query::QueryEngine::<UpdateOperator>::new(update_query_object);
        match maybe_filter_query {
            Some(q) => {
                let filter_query: Value = depythonize(q).unwrap();
                let filter_query_object: &Map<String, Value> = filter_query.as_object().unwrap();
                let filter_query_engine =
                    query::QueryEngine::<QueryOperator>::new(filter_query_object);
                collection_values.iter_mut().for_each(|c| {
                    let c_obj = c.as_object_mut().unwrap();
                    if filter_query_engine.execute(c_obj).unwrap() {
                        // Do update
                        update_query_engine.execute(c_obj)
                    }
                })
            }
            None => collection_values.iter_mut().for_each(|c| {
                let c_obj = c.as_object_mut().unwrap();
                update_query_engine.execute(c_obj)
            }),
        };
        Ok(collection_values)
    }

    fn _write(&self, collection_name: &str, document: &Vec<Value>) -> Result<(), PyErr> {
        let path = self.get_collection_path(collection_name);
        let temp_path = format!("{}.tmp", collection_name); // Temporary file
        let temp_file_result = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path);

        let temp_file = match temp_file_result {
            Ok(file) => file,
            Err(err) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Problem creating temporary file: {err:?}"
                )));
            }
        };

        let mut writer = BufWriter::new(temp_file);
        match serde_json::to_writer(&mut writer, &document) {
            Ok(_) => {
                writer.flush().unwrap();
            }
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Error serializing JSON",
                ));
            }
        };

        match rename(&temp_path, &path) {
            Ok(_) => Ok(()),
            Err(err) => Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Error renaming file: {err:?}"
            ))),
        }
    }
}

#[pymethods]
impl Bison {
    #[new]
    #[pyo3(signature = (name, document_name = None))]
    pub fn new(name: String, document_name: Option<String>) -> PyResult<Self> {
        let base_path = PathBuf::from(name.clone());
        if !base_path.exists() {
            let _ = fs::create_dir(&base_path);
        }

        let collections = serde_json::Map::new();
        let query_cache = LruCache::new(query::QUERY_CACHE_SIZE);
        let mut db = Bison {
            base_path,
            collections,
            query_cache,
        };

        match document_name {
            Some(document_name) => {
                // Initializes a database from an existing document
                let document: Map<String, Value> = Bison::read_document(&document_name)?
                    .as_object()
                    .unwrap()
                    .to_owned();
                for (key, value) in document {
                    db.insert_in_collection(&key, value)?
                }
                Ok(db)
            }
            None => {
                // TODO: go check in storage which tables are there
                Ok(db)
            }
        }
    }
    pub fn create_collection(&mut self, collection_name: &str) -> PyResult<()> {
        let path = self.get_collection_path(collection_name);
        if path.exists() {
            return Ok(());
        }
        // Create a file to save the JSON data
        let mut file = File::create(&path)?;

        // Write the JSON data to the file
        let json_data = format!("{{ \"{}\":[] }}", collection_name);
        file.write_all(json_data.as_bytes())?;
        self.collections
            .insert(collection_name.to_string(), Value::Array(vec![]));
        Ok(())
    }

    pub fn insert(
        &mut self,
        collection_name: String,
        document: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let obj: Value = depythonize(document).unwrap();
        self.insert_in_collection(&collection_name, obj)
    }

    pub fn insert_many(
        &mut self,
        collection_name: String,
        documents: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let obj: Value = depythonize(documents).unwrap();
        self.insert_in_collection(&collection_name, obj)
    }

    pub fn insert_many_from_document(
        &mut self,
        collection_name: String,
        document_name: String,
    ) -> PyResult<()> {
        // Insert many from json (array document)
        // The top most object in the json document
        // should be an array
        let values: Value = Bison::read_document(&document_name)?;
        match values.as_array() {
            // Here we do not insert the array as we are making that distinction in
            // Bison::insert_in_collection already
            Some(_) => self.insert_in_collection(&collection_name, values),
            None => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Document is not an array",
            )),
        }
    }

    #[pyo3(signature = (collection_name, maybe_query = None))]
    pub fn find(
        &mut self,
        collection_name: String,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let found_collections = self._find(&collection_name, maybe_query)?;

        let py_collections = {
            let mut result: Option<PyObject> = None;
            let mut py_error: Option<PyErr> = None;

            Python::with_gil(|py| {
                match pythonize(py, &found_collections) {
                    Ok(obj) => {
                        // Convert &PyAny to PyObject
                        let py_obj = obj.to_object(py);
                        result = Some(py_obj);
                    }
                    Err(err) => py_error = Some(err.into()),
                }
            });

            if let Some(err) = py_error {
                return Err(err);
            }
            result.expect("Failed to obtain PyObject")
        };

        Ok(py_collections)
    }

    #[pyo3(signature = (collection_name, update_query, maybe_query = None))]
    pub fn update(
        &mut self,
        collection_name: String,
        update_query: &Bound<'_, PyDict>,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        // Reset cache after every update
        // TODO: Hardcoded cache size
        self.query_cache = LruCache::new(query::QUERY_CACHE_SIZE);

        let in_memory_collection = self.collections.get(&collection_name);
        let collection_values: Cow<Vec<Value>> = match in_memory_collection {
            Some(c) => Cow::Borrowed(c.as_array().unwrap()),
            None => match self.find_collection_in_storage(&collection_name) {
                Ok(in_storage_collection) => Cow::Owned(in_storage_collection),
                Err(err) => return Err(err),
            },
        };

        let updated_collections = self
            ._update(collection_values.to_vec(), update_query, maybe_query)
            .unwrap();

        let py_collections = {
            let mut result: Option<PyObject> = None;
            let mut py_error: Option<PyErr> = None;

            Python::with_gil(|py| {
                match pythonize(py, &updated_collections) {
                    Ok(obj) => {
                        // Convert &PyAny to PyObject
                        let py_obj = obj.to_object(py);
                        result = Some(py_obj);
                    }
                    Err(err) => py_error = Some(err.into()),
                }
            });

            if let Some(err) = py_error {
                return Err(err);
            }
            result.expect("Failed to obtain PyObject")
        };

        self.collections.insert(
            collection_name,
            serde_json::Value::Array(updated_collections),
        );
        Ok(py_collections)
    }

    pub fn collections(&self) -> PyResult<Vec<String>> {
        // Get collection names
        let entries = fs::read_dir(self.base_path.as_path())?
            .map(|res| {
                res.map(|e| {
                    e.path()
                        .file_stem()
                        .unwrap()
                        .to_os_string()
                        .into_string()
                        .unwrap()
                })
            })
            .collect::<Result<Vec<_>, io::Error>>()?;

        Ok(entries)
    }

    pub fn drop_collection(&mut self, collection_name: String) -> PyResult<()> {
        let path = self.get_collection_path(&collection_name);
        let _ = fs::remove_file(path);
        self.collections.remove_entry(&collection_name);
        Ok(())
    }

    pub fn drop_all(&mut self) -> PyResult<()> {
        let _ = self
            .collections()
            .unwrap()
            .into_iter()
            .try_for_each(|collection_name| self.drop_collection(collection_name));
        let _ = fs::remove_dir(self.base_path.clone());
        Ok(())
    }
    pub fn write(&self, collection_name: String) -> PyResult<()> {
        match self.collections.get(&collection_name) {
            Some(collection) => self._write(&collection_name, collection.as_array().unwrap()),
            None => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Collection '{}' not found in stored collections",
                collection_name
            ))),
        }
    }

    pub fn write_all(&self) -> PyResult<()> {
        let _ = self
            .collections
            .iter()
            .map(|(collection_name, values)| {
                // TODO: Probably need to return the PyErr in case it happens
                let _ = self._write(collection_name, values.as_array().unwrap());
            })
            .collect::<Vec<_>>();

        Ok(())
    }
    pub fn clear_cache(&mut self) -> PyResult<()> {
        self.query_cache = LruCache::new(query::QUERY_CACHE_SIZE);
        Ok(())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn bison(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Bison>()?;
    Ok(())
}
