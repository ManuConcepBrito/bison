use lru::LruCache;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::PyErr;
use pyo3::PyObject;
use pythonize::{depythonize, pythonize};
use query::{QueryOperator, UpdateOperator};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::io::BufWriter;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

mod query;

#[derive(Debug)]
#[pyclass]
pub struct Bison {
    base_path: PathBuf,
    collections: HashMap<String, Arc<RwLock<Vec<Value>>>>,
    query_cache: LruCache<u64, Arc<RwLock<Vec<Value>>>>,
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
    fn update_in_memory_collections(&mut self, collection_name: &str) -> Result<(), PyErr> {
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
        let values_in_storage: Value = Bison::read_document(collection_path.to_str().unwrap())?;
        let collection = values_in_storage.get(collection_name).unwrap();
        let collection_arr = collection.as_array().unwrap();
        let collection_arc = Arc::new(RwLock::new(collection_arr.clone()));
        self.collections
            .insert(collection_name.to_string(), collection_arc);
        Ok(())
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

        let collection_arc = self.collections.get(collection_name).unwrap();

        {
            let mut collection = collection_arc.write().unwrap();
            // Extend the collection if the value to insert is an array
            if let Some(insert_value_arr) = insert_value.as_array() {
                collection.extend_from_slice(insert_value_arr)
            } else {
                collection.push(insert_value);
            }
        }

        self.collections
            .insert(collection_name.to_string(), collection_arc.clone());
        Ok(())
    }

    fn _find(
        &mut self,
        collection_name: &str,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> Result<Arc<RwLock<Vec<Value>>>, PyErr> {
        // Inner method that returns Vec<Value> instead
        // of a python dict
        let in_memory_collection = self.collections.get(collection_name);
        let collection_arc = match in_memory_collection {
            Some(c) => c,
            None => match self.update_in_memory_collections(collection_name) {
                Ok(_) => self.collections.get(collection_name).unwrap(),
                Err(err) => return Err(err),
            },
        };

        let query: Value = match maybe_query {
            Some(q) => depythonize(q).unwrap(),
            None => {
                // If there is no query, return all the values
                return Ok(collection_arc.clone());
            }
        };

        let query_object: &Map<String, Value> = query.as_object().unwrap();
        let mut hasher = DefaultHasher::new();
        query_object.hash(&mut hasher);
        let query_hash = hasher.finish();
        if let Some(cached_collections) = self.query_cache.get(&query_hash) {
            return Ok(cached_collections.clone());
        }
        let query_engine = query::QueryEngine::<QueryOperator>::new(query_object);
        // execute queries and return collections
        let mut found_collections: Vec<Value> = vec![];
        let read_collections = collection_arc.read().unwrap();
        for collection in read_collections.iter() {
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
        let found_collections_arc = Arc::new(RwLock::new(found_collections));
        self.query_cache
            .put(query_hash, found_collections_arc.clone());
        Ok(found_collections_arc)
    }
    fn _update(
        &mut self,
        collection_name: &str,
        py_update_query: &Bound<'_, PyDict>,
        maybe_filter_query: Option<&Bound<'_, PyDict>>,
    ) -> Result<Arc<RwLock<Vec<Value>>>, PyErr> {
        let in_memory_collection = self.collections.get(collection_name);
        let collection_values_arc = match in_memory_collection {
            Some(c) => c,
            None => match self.update_in_memory_collections(&collection_name) {
                Ok(_) => self.collections.get(collection_name).unwrap(),
                Err(err) => return Err(err),
            },
        };
        {
            let mut collection_values = collection_values_arc.write().unwrap();
            let update_query: Value = depythonize(py_update_query).unwrap();
            let update_query_object: &Map<String, Value> = update_query.as_object().unwrap();
            let update_query_engine =
                query::QueryEngine::<UpdateOperator>::new(update_query_object);
            match maybe_filter_query {
                Some(q) => {
                    let filter_query: Value = depythonize(q).unwrap();
                    let filter_query_object: &Map<String, Value> =
                        filter_query.as_object().unwrap();
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
        }
        Ok(collection_values_arc.clone())
    }

    fn _write(
        &self,
        collection_name: &str,
        document: Arc<RwLock<Vec<Value>>>,
    ) -> Result<(), PyErr> {
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
        let doc: &Vec<Value> = &document.read().unwrap();
        match serde_json::to_writer(&mut writer, doc) {
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
    #[pyo3(signature = (name))]
    pub fn new(name: String) -> PyResult<Self> {
        let base_path = PathBuf::from(name.clone());
        let collections = HashMap::new();
        let query_cache = LruCache::new(query::QUERY_CACHE_SIZE);
        let mut db = Bison {
            base_path: base_path.clone(),
            collections,
            query_cache,
        };
        if !base_path.exists() {
            let _ = fs::create_dir(&base_path);
        } else if base_path.exists() {
            let json = &OsStr::new("json");

            // want all files with a .json extension
            let entries = Vec::from_iter(
                fs::read_dir(&db.base_path)?
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .filter(|p| p.extension() == Some(json)),
            );
            for entry in entries {
                let collection_path = entry.to_str().unwrap();
                let collection_name = entry.file_stem().unwrap().to_str().unwrap();
                // This is {[values]} if written by bison

                let collection_in_storage: Vec<Value> = Bison::read_document(collection_path)?
                    .as_array()
                    .unwrap()
                    .to_owned();
                db.collections.insert(
                    collection_name.to_string(),
                    Arc::new(RwLock::new(collection_in_storage)),
                );
            }
        } else {
            // TODO: Remove this and move to fn
        }
        Ok(db)
    }
    pub fn load_from_document(&mut self, document_path: &str) -> PyResult<()> {
        // Initializes a database from an existing document
        let document: Map<String, Value> = Bison::read_document(&document_path)?
            .as_object()
            .unwrap()
            .to_owned();
        for (key, value) in document {
            self.insert_in_collection(&key, value)?
        }
        return Ok(());
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
        let empty_collection: Arc<RwLock<Vec<Value>>> = Arc::new(RwLock::new(Vec::new()));
        self.collections
            .insert(collection_name.to_string(), empty_collection);
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
                match pythonize(py, found_collections.as_ref()) {
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

    #[pyo3(signature = (collection_name, update_query, maybe_query = None, return_result=false))]
    pub fn update(
        &mut self,
        collection_name: String,
        update_query: &Bound<'_, PyDict>,
        maybe_query: Option<&Bound<'_, PyDict>>,
        return_result: bool,
    ) -> PyResult<Option<PyObject>> {
        // Reset cache after every update
        self.query_cache = LruCache::new(query::QUERY_CACHE_SIZE);

        let updated_collections = self
            ._update(&collection_name, update_query, maybe_query)
            .unwrap();

        let return_value = match return_result {
            true => {
                let py_collections = {
                    let mut result: Option<PyObject> = None;
                    let mut py_error: Option<PyErr> = None;

                    Python::with_gil(|py| {
                        match pythonize(py, updated_collections.as_ref()) {
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
                Some(py_collections)
            }
            false => Option::None,
        };
        self.collections
            .insert(collection_name, updated_collections);
        return Ok(return_value);
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
            Some(collection) => self._write(&collection_name, collection.clone()),
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
                let _ = self._write(collection_name, values.clone());
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
