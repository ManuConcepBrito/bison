use jiter::{map_json_error, PartialMode, PythonParse, StringCacheMode};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyObject;
use pythonize::{depythonize, pythonize};
use serde_json::{to_vec, Map, Result, Value};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{read, read_to_string, rename};
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use uuid::Uuid;

trait Query: Debug {
    fn execute(&self, collection: Map<String, Value>) -> bool;
}

#[derive(Debug)]
struct EqualQuery {
    fields: Vec<String>,
    value: Value,
}

impl Query for EqualQuery {
    fn execute(&self, collection: Map<String, Value>) -> bool {
        // TODO
        false
    }
}

#[derive(Debug, Clone)]
enum QueryEngineError {
    InvalidQueryFormat,
}

#[derive(Debug)]
struct QueryEngine {
    queries: Vec<Box<dyn Query>>,
}

pub fn _parse_query(sub_query: &Value, key: &str, fields: &mut Vec<String>) -> Value {
    fields.push(key.to_string());
    let value: Value = match sub_query {
        Value::Object(map) => map
            .into_iter()
            .map(|(key, val)| return _parse_query(val, key, fields))
            .collect(),
        Value::Bool(b) => Value::Bool(*b),
        Value::Number(n) => Value::Number(n.clone()),
        Value::String(s) => Value::String(s.to_string()),
        _ => panic!("Not Valid query"),
    };
    return value;
}
impl QueryEngine {
    pub fn new(unparsed_query: &Map<String, Value>) -> Self {
        let queries: Vec<Box<dyn Query>> = unparsed_query
            .into_iter()
            .map(|(key, sub_query)| {
                let mut fields = Vec::new();
                let value = _parse_query(sub_query, key, &mut fields);
                return Box::new(EqualQuery { fields, value }) as Box<dyn Query>;
            })
            .collect();
        QueryEngine { queries }
    }
}
// pub fn parse_query(unparsed_query: &Value) -> Value {
// let mut fields: Vec<String> = Vec::new();
// let mut values: Vec<Values> = Vec::new();
// match unparsed_query {
//     Value::Object(map) => {
//         for (key, val) in map {
//             fields.push(key.to_string);
//             match val {
//                 Value::Object(inner) =>
//             }
//         }
//     }
//
// }
// let mut value: Value = match unparsed_query {
//     Value::Object(map) => {
//         for (key, val) in map {
//             fields.push(key.to_string());
//             return parse_query(val.into(), fields)
//         }
//     },
//     Value::Bool(b) => b,
//     Value::String(s) => s,
//     Value::Number(n) => n,
//     _ => panic!("Invalid Query")
// };
// return value

/*
* Simple query: {"a": 3} => a is equal to 3
* Concatenated simple queries {"a": 3, "b": 4} a == 3 AND b == 3
* {"a": {"$ne": 3}, "b": 3}
* Query building:
*
* Query object => Each condition in a query is a separate query object
* QueryEngine({"a": 3, "b": 4, "c": {"$ne": 4} }) =>
* Query({"a": 3}), Query({"b": 4}, Query({"c": {"$ne": 4} })
*
*
*
* Each query has an execute fn which takes
* a Map<String, Value> (collection) and returns a
* bool if the condition is true
*
*
*
*
* Naive solution: O(m * n) m = number of keys in query and n = number of documents in table
* found = False
* found_elements_idx = []
* for key, value in query:
*     for idx, collection in collections:
*       if collection[key] == value:
*           found_elements_idx.append(idx)
*
*
*
*
*
* */

// fn value_to_object( val: &Value, py: Python<'_> ) -> PyObject {
//     match val {
//         Value::Null => py.None(),
//         Value::Bool( x ) => x.to_object( py ),
//         Value::Number( x ) => {
//             let oi64 = x.as_i64().map( |i| i.to_object( py ) );
//             let ou64 = x.as_u64().map( |i| i.to_object( py ) );
//             let of64 = x.as_f64().map( |i| i.to_object( py ) );
//             oi64.or( ou64 ).or( of64 ).expect( "number too large" )
//         },
//         Value::String( x ) => x.to_object( py ),
//         Value::Array( x ) => {
//             let inner: Vec<_> = x.iter().map(|x| value_to_object(x, py)).collect();
//             inner.to_object( py )
//         },
//         Value::Object( x ) => {
//             let inner: HashMap<_, _> =
//                 x.iter()
//                     .map( |( k, v )| ( k, value_to_object( v, py ) ) ).collect();
//             inner.to_object( py )
//         },
//     }
// }
//
// #[repr(transparent)]
// #[derive( Clone, Debug )]
// struct ParsedValue( Value );
//
// impl ToPyObject for ParsedValue {
//     fn to_object( &self, py: Python<'_> ) -> PyObject {
//         value_to_object( &self.0, py )
//     }
// }

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
}
#[pymethods]
impl Bison {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        let base_path = PathBuf::from(name.clone());
        if !base_path.exists() {
            let _ = fs::create_dir(&base_path);
        }
        // TODO: go check in storage which tables are there
        let collections = HashMap::new();
        Ok(Bison {
            base_path,
            collections,
        })
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
                    "Problem accessing collection: {err:?}"
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
                    "Problem accessing collection: {err:?}"
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
        let mut query: Value = match maybe_query {
            Some(q) => depythonize(q).unwrap(),
            None => {
                let py_collections = {
                    let mut result: Option<PyObject> = None;
                    let mut py_error: Option<PyErr> = None;

                    Python::with_gil(|py| {
                        match pythonize(py, &parsed) {
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
        println!("Query engine is {:?}", query_engine);
        // let _: Vec<_> = query_object
        //     .into_iter()
        //     .map(|(key, sub_query)| {
        //         let mut fields = Vec::new();
        //         let value = _parse_query(sub_query, key, &mut fields);
        //         println!("Value found {:?}", value);
        //         println!("Fields found {:?}", fields);
        //     })
        //     .collect();

        // if let Some(Value::Array(collections)) = parsed.get_mut(collection_name) {
        //     collections.iter().find()
        //
        // }

        let py_collections = {
            let mut result: Option<PyObject> = None;
            let mut py_error: Option<PyErr> = None;

            Python::with_gil(|py| {
                match pythonize(py, &parsed) {
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
        // 2) support simple queries {key: value}

        // 3) support projections, specify key: 1 or 0. If value is 0 field is not shown, if value
        //    is 1 field is shown

        // 4)
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
    // pub fn update_key(&mut self, _key: &str, _new_value: &str) -> PyResult<Option<usize>> {
    //     loop {
    //         // Fill the buffer (immutable borrow)
    //         let data_len;
    //         {
    //             let data = self.reader.fill_buf()?;
    //
    //             if data.is_empty() {
    //                 break;
    //             }
    //             // Get the length of the data to consume
    //             data_len = data.len();
    //             // TODO: All processing needs to go in here
    //         } // Immutable borrow ends here (data goes out of scope)
    //
    //         // Now it's safe to mutably borrow self.reader for consuming
    //         self.reader.consume(data_len);
    //     }
    //     Ok(Some(2))
    // }
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
