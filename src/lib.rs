use jiter::{map_json_error, PartialMode, PythonParse, StringCacheMode};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyErr;
use pyo3::PyObject;
use pythonize::{depythonize, pythonize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{read, read_to_string, rename};
use std::io;
use std::io::BufWriter;
use std::io::{BufReader, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug)]
enum QueryOperator {
    Equal,
    NotEqual,
    // GreaterThan,
    // GreaterThanEqual,
    // LessThan,
    // LessThanEqual,
}

impl FromStr for QueryOperator {
    type Err = ();

    fn from_str(query_op: &str) -> Result<QueryOperator, Self::Err> {
        match query_op {
            "$eq" => Ok(QueryOperator::Equal),
            "$ne" => Ok(QueryOperator::NotEqual),
            // "$gt" => Ok(QueryOperator::GreaterThan),
            // "$gte" => Ok(QueryOperator::GreaterThanEqual),
            // "$lt" => Ok(QueryOperator::LessThan),
            // "$lte" => Ok(QueryOperator::LessThanEqual),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
struct Query {
    fields: Vec<String>,
    value: Value,
    operator: QueryOperator,
}

impl Query {
    fn execute(&self, collection: &Map<String, Value>) -> bool {
        let mut current_value = collection;

        for key in &self.fields[..self.fields.len() - 1] {
            match current_value.get(key) {
                Some(Value::Object(map)) => {
                    current_value = map;
                }
                _ => {
                    return false;
                }
            }
        }
        let last_key = &self.fields[self.fields.len() - 1];
        match current_value.get(last_key) {
            Some(value) => self._execute_operator(value),
            None => false,
        }
    }

    fn _execute_operator(&self, last_value: &Value) -> bool {
        match self.operator {
            QueryOperator::Equal => &self.value == last_value,
            QueryOperator::NotEqual => &self.value != last_value,
            // QueryOperator::LessThan => &self.value < last_value,
            // QueryOperator::GreaterThan => &self.value > last_value,
            // QueryOperator::LessThanEqual => &self.value <= last_value,
            // QueryOperator::GreaterThanEqual => &self.value >= last_value
        }
    }
}

#[derive(Debug)]
struct QueryEngine {
    queries: Vec<Query>,
}

impl QueryEngine {
    pub fn new(unparsed_query: &Map<String, Value>) -> Self {
        // Compile an unparsed query into a list of queries.
        //
        // # Examples
        //
        // let query_engine = QueryEngine({"a": 10}) // Query to search for a = 10
        // let query_engine = QueryEngine({"a": {"eq": 10}}) // Equivalent query
        // let query_engine = QueryEgine({"a": {"b": 100}}) // Nested query
        //
        let queries: Vec<Query> = unparsed_query
            .into_iter()
            .map(|(key, sub_query)| {
                let mut fields: Vec<String> = Vec::new();
                let value = _parse_query(sub_query, key, &mut fields);
                // if no '$' operator is found, assume it is an EqualOperator
                // For example: {"a": 10} => a == 10
                let mut query_op = QueryOperator::Equal;
                if fields.last().unwrap().chars().next() == Some('$') {
                    let query_op_str = fields.pop().unwrap();
                    // TODO: Error should be a python error
                    query_op = QueryOperator::from_str(&query_op_str)
                        .expect(&format!("Unknown query operator found: {}", query_op_str));
                    println!("Found operator: {:?}", query_op);
                }
                return Query {
                    fields,
                    value,
                    operator: query_op,
                };
            })
            .collect();
        QueryEngine { queries }
    }
    fn execute(&self, collection: &Map<String, Value>) -> bool {
        let query_iter = self.queries.iter();
        for q in query_iter {
            let query_result = q.execute(collection);
            if !query_result {
                return false;
            }
        }
        return true;
    }
}

pub fn _parse_query(sub_query: &Value, key: &str, fields: &mut Vec<String>) -> Value {
    /*
     * Parses a query recursively. It extracts the fields and value involved in a query.
     * When there is a nested query like {"a": {"b": 10} } it will extract the fields
     * ["a", "b"] to be able to follow the collection and return the value (10).
     */
    fields.push(key.to_string());
    let value = match sub_query {
        Value::Object(map) => map
            .into_iter()
            .find_map(|(key, val)| return Some(_parse_query(val, key, fields)))
            .expect("Error while parsing query"),
        Value::Bool(b) => Value::Bool(*b),
        Value::Number(n) => Value::Number(n.clone()),
        Value::String(s) => Value::String(s.to_string()),
        _ => panic!("Not Valid query"),
    };

    return value;
}

pub fn extract_collection(file: File, collection_name: String) -> Result<Vec<Value>, PyErr> {
    let reader = BufReader::new(file);

    // Deserialize JSON from the reader
    let parsed: Value = serde_json::from_reader(reader)
        .map_err(|_| PyErr::new::<PyValueError, _>("Error deserializing JSON"))?;

    // Ensure the parsed JSON is an object and get a reference to it
    let collection = parsed
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

#[pyclass]
pub struct Bison {
    base_path: PathBuf,
    collections: HashMap<String, Collection>,
}

impl Bison {
    fn get_collection_path(&self, collection_name: &str) -> PathBuf {
        let mut path = self.base_path.clone();
        path.push(&collection_name);
        path.set_extension("json");
        path
    }

    fn get_top_level_keys(document_name: String) -> Result<Vec<String>, PyErr> {
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

        // Get the top-level keys if the JSON is an object
        if let Value::Object(map) = json_value {
            let keys = map.keys().cloned().collect();
            Ok(keys)
        } else {
            Err(PyErr::new::<PyValueError, _>(
                "The JSON root is not an object",
            ))
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

        let collections = HashMap::new();
        let mut db = Bison { base_path,
                    collections,
                };
        match document_name {
            Some(document_name) => {
                // Initializes a database from an existing document
                let collection_names: Vec<String> = Bison::get_top_level_keys(document_name)?;
                let _ = collection_names.into_iter().map(|collection_name| db.create_collection(collection_name)).collect::<Vec<_>>();
                Ok(db)
            }
            None => {
                // TODO: go check in storage which tables are there
                Ok(db)
            }
        }
    }
    pub fn create_collection(&mut self, collection_name: String) -> PyResult<()> {
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
        self.collections.insert(collection_name.clone(), collection);
        Ok(())
    }

    pub fn insert(
        &mut self,
        collection_name: String,
        _document: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let path = self.get_collection_path(&collection_name);
        let temp_path = format!("{}.tmp", &collection_name); // Temporary file
        let file_result = OpenOptions::new().read(true).open(&path);

        let file = match file_result {
            Ok(file) => file,
            Err(err) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Error opening collection '{}': {}",
                    collection_name, err
                )));
            }
        };

        let reader = BufReader::new(file);
        let mut parsed: Value = match serde_json::from_reader(reader) {
            Ok(data) => data,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Error deserializing JSON",
                ));
            }
        };

        let mut obj: Value = depythonize(&_document).unwrap();
        let uuid = Uuid::new_v4();
        parsed.as_object_mut().unwrap();
        obj.as_object_mut()
            .unwrap()
            .insert("_id".to_string(), Value::String(uuid.to_string()));

        if let Some(Value::Array(arr)) = parsed.get_mut(collection_name) {
            arr.push(obj);
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
        match serde_json::to_writer(&mut writer, &parsed) {
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
    #[pyo3(signature = (collection_name, maybe_query = None))]
    pub fn find(
        &mut self,
        collection_name: String,
        maybe_query: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        // 1) If not query return all elements in collection

        let path = self.get_collection_path(&collection_name);
        let file_result = OpenOptions::new().read(true).open(&path);

        let file = match file_result {
            Ok(file) => file,
            Err(err) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Error opening collection '{}': {}",
                    collection_name, err
                )));
            }
        };
        let collection = extract_collection(file, collection_name).unwrap();
        let query: Value = match maybe_query {
            Some(q) => depythonize(q).unwrap(),
            None => {
                let py_collections = {
                    let mut result: Option<PyObject> = None;
                    let mut py_error: Option<PyErr> = None;

                    Python::with_gil(|py| {
                        match pythonize(py, &collection) {
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
                return Ok(py_collections);
            }
        };
        let query_object: &Map<String, Value> = query.as_object().unwrap();
        let query_engine = QueryEngine::new(query_object);
        // execute queries and return collections
        let found_collections: Vec<Value> = collection
            .into_iter()
            .filter(|c| {
                let c_obj = c.as_object().unwrap();
                let result: bool = query_engine.execute(c_obj);
                result
            })
            .collect();
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

        // 3) support projections, specify key: 1 or 0. If value is 0 field is not shown, if value
        //    is 1 field is shown

        // 4)
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
