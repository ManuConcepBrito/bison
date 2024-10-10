use jiter::{map_json_error, PartialMode, PythonParse, StringCacheMode};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::PyErr;
use pyo3::PyObject;
use pythonize::{depythonize, pythonize};
use query::{QueryOperator, UpdateOperator};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{read, read_to_string, rename};
use std::io;
use std::io::BufWriter;
use std::io::{BufReader, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

mod query;

#[pyclass]
pub struct Bison {
    base_path: PathBuf,
    collections: HashMap<String, Collection>,
}
// TODO(manuel): Implement update operations
// delete, increment, decrement, add, subtract, set
// for reference: https://github.com/msiemens/tinydb/blob/master/tests/test_operations.py
impl Bison {
    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(&collection_name);
        path.set_extension("json");
        path
    }

    fn read_document(document_name: String) -> Result<Value, PyErr> {
        let file_path = PathBuf::from(document_name.clone());
        let file_result = OpenOptions::new().read(true).open(&file_path);

        let file = match file_result {
            Ok(file) => file,
            Err(_err) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(
                    "Error opening document",
                ));
            }
        };
        let reader = BufReader::new(file);

        // Parse the file into a serde_json::Value
        let json_value: Value = serde_json::from_reader(reader)
            .map_err(|_| PyErr::new::<PyValueError, _>("Error deserializing JSON"))?;
        // TODO(manuel): Better way than cloning here?
        // Ok(json_value.as_object().unwrap().to_owned())
        Ok(json_value)
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
        if !self
            .collections()
            .unwrap()
            .contains(&collection_name.to_string())
        {
            let _ = self.create_collection(collection_name);
        }
        let path = self.get_collection_path(&collection_name);
        // Read the existing collection/document
        let mut document: Map<String, Value> =
            Bison::read_document(path.to_str().unwrap().to_string())?
                .as_object()
                .unwrap()
                .to_owned();

        let temp_path = format!("{}.tmp", &collection_name); // Temporary file
                                                             // TODO(manuel): Do you even need this?
                                                             // let uuid = Uuid::new_v4();
                                                             // insert_value.insert("_id".to_string(), Value::String(uuid.to_string()));

        if let Some(Value::Array(arr)) = document.get_mut(collection_name) {
            // Extend the collection if the value to insert is an array
            if let Some(insert_value_arr) = insert_value.as_array() {
                arr.extend_from_slice(insert_value_arr)
            } else {
                arr.push(insert_value);
            }
        }

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

    fn _find(
        &mut self,
        collection_values: Vec<Value>,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> Result<Vec<Value>, PyErr> {
        // Inner method that returns Vec<Value> instead
        // of a python dict
        let query: Value = match maybe_query {
            Some(q) => depythonize(q).unwrap(),
            None => {
                // If there is no query, return all the values
                return Ok(collection_values);
            }
        };
        let query_object: &Map<String, Value> = query.as_object().unwrap();
        let query_engine = query::QueryEngine::<QueryOperator>::new(query_object);
        // execute queries and return collections
        let found_collections: Vec<Value> = collection_values
            .into_iter()
            .filter(|c| {
                let c_obj = c.as_object().unwrap();
                let result: bool = query_engine.execute(c_obj);
                result
            })
            .collect();
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
                let filter_query_engine = query::QueryEngine::<QueryOperator>::new(filter_query_object);
                collection_values.iter_mut().for_each(|c| {

                let c_obj = c.as_object_mut().unwrap();
                if filter_query_engine.execute(c_obj) {
                    // Do update
                    update_query_engine.execute(c_obj)
                    }
                })

            },
            None => {
                collection_values.iter_mut().for_each(|c| {

                let c_obj = c.as_object_mut().unwrap();
                update_query_engine.execute(c_obj)
                })
            }
        };
        Ok(collection_values)
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

        let collections = HashMap::new();
        let mut db = Bison {
            base_path,
            collections,
        };
        match document_name {
            Some(document_name) => {
                // Initializes a database from an existing document
                let document: Map<String, Value> = Bison::read_document(document_name)?
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
        let path = self.get_collection_path(&collection_name);
        if path.exists() {
            return Ok(());
        }
        // Create a file to save the JSON data
        let mut file = File::create(&path)?;

        // Write the JSON data to the file
        let json_data = format!("{{ \"{}\":[] }}", collection_name);
        file.write_all(json_data.as_bytes())?;
        let collection =
            Collection::new(path.to_str().unwrap_or("Error unwrapping collection name"))?;
        self.collections
            .insert(collection_name.to_string(), collection);
        Ok(())
    }

    pub fn insert(
        &mut self,
        collection_name: String,
        document: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let obj: Value = depythonize(&document).unwrap();
        self.insert_in_collection(&collection_name, obj)
    }

    pub fn insert_many(
        &mut self,
        collection_name: String,
        documents: &Bound<'_, PyList>,
    ) -> PyResult<()> {
        let obj: Value = depythonize(&documents).unwrap();
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
        let values: Value = Bison::read_document(document_name)?;
        match values.as_array() {
            // Here we do not insert the array as we are making that distinction in
            // Bison::insert_in_collection already
            Some(_) => self.insert_in_collection(&collection_name, values),
            None => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Document is not an array",
                ))
            }
        }
    }

    #[pyo3(signature = (collection_name, maybe_query = None))]
    pub fn find(
        &mut self,
        collection_name: String,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let path = self.get_collection_path(&collection_name);
        // Raw collection is the read document {"collection_name": [{...}, {...}, etc]}
        let raw_collection = Bison::read_document(path.to_str().unwrap().to_string()).unwrap();
        // Collection values are the values of "collection_name"
        let collection_values = Bison::extract_collection(raw_collection, collection_name).unwrap();

        let found_collections = self._find(collection_values, maybe_query).unwrap();

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
        let path = self.get_collection_path(&collection_name);
        // Raw collection is the read document {"collection_name": [{...}, {...}, etc]}
        let raw_collection = Bison::read_document(path.to_str().unwrap().to_string()).unwrap();
        // Collection values are the values of "collection_name"
        let collection_values = Bison::extract_collection(raw_collection, collection_name).unwrap();
        println!("Before update {:?}", collection_values);
        let updated_collections = self._update(collection_values, update_query, maybe_query).unwrap();

        println!("After update {:?}", updated_collections);

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
        Ok(())
    }

    pub fn drop_all(&mut self) -> PyResult<()> {
        let _ = self
            .collections()
            .unwrap()
            .into_iter()
            .map(|collection_name| self.drop_collection(collection_name))
            .collect::<Result<(), PyErr>>();
        let _ = fs::remove_dir(self.base_path.clone());
        Ok(())
    }
}

#[pyclass]
pub struct Collection {
    writer: BufWriter<File>,
}

#[pymethods]
impl Collection {
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(Collection {
            writer: BufWriter::new(file),
        })
    }
}

#[pyfunction]
fn _replace_at_index_in_place(file_path: &str, offset: u64, new_value: &str) -> PyResult<bool> {
    // Open the file for read and write
    let file = OpenOptions::new().read(true).write(true).open(file_path)?;
    let _ = file.write_at(new_value.as_bytes(), offset);
    Ok(true)
}

#[pyfunction]
fn _find_key(file: String, key: &str) -> PyResult<bool> {
    let data = read_to_string(file)?;

    let value: Value = serde_json::from_str(&data).unwrap_or(Value::Bool(false));
    if value == Value::Bool(false) {
        return Ok(false);
    }

    if let Some(obj) = value.as_object() {
        if obj.contains_key(key) {
            return Ok(true);
        } else {
            return Ok(false);
        }
    } else {
        return Ok(false);
    }
}

#[pyfunction]
fn read_python<'py>(py: Python<'py>, file: String) -> PyResult<Bound<'py, PyAny>> {
    let json_bytes = read(file)?;
    let parse_builder = PythonParse {
        allow_inf_nan: false,
        partial_mode: PartialMode::Off,
        cache_mode: StringCacheMode::Keys,
        catch_duplicate_keys: false,
        lossless_floats: false,
    };
    parse_builder
        .python_parse(py, &json_bytes)
        .map_err(|e| map_json_error(&json_bytes, &e))
}

/// A Python module implemented in Rust.
#[pymodule]
fn bison(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_python, m)?)?;
    m.add_function(wrap_pyfunction!(_find_key, m)?)?;
    m.add_function(wrap_pyfunction!(_replace_at_index_in_place, m)?)?;
    m.add_class::<Collection>()?;
    m.add_class::<Bison>()?;
    Ok(())
}
