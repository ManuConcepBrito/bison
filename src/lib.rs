use jiter::{map_json_error, PartialMode, PythonParse, StringCacheMode};
use pyo3::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{read, read_to_string};
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::FileExt;


#[pyclass]
pub struct Document {
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

#[pymethods]
impl Document {
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(Document {
            reader: BufReader::new(file.try_clone()?),
            writer: BufWriter::new(file),
        })
    }
    pub fn update_key(&mut self, _key: &str, _new_value: &str) -> PyResult<Option<usize>> {
        loop {
            // Fill the buffer (immutable borrow)
            let data_len;
            {
                let data = self.reader.fill_buf()?;

                if data.is_empty() {
                    break;
                }
                // Get the length of the data to consume
                data_len = data.len();
                // TODO: All processing needs to go in here
            } // Immutable borrow ends here (data goes out of scope)

            // Now it's safe to mutably borrow self.reader for consuming
            self.reader.consume(data_len);
        }
        Ok(Some(2))
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
    m.add_class::<Document>()?;
    Ok(())
}
