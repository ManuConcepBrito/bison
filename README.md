<p align="center">
<img src="https://github.com/user-attachments/assets/9ba39171-29d6-4d4c-8ab8-fd1c9687b83d" alt="Bison"></a>
</p>

# Bison

Bison is a NoSQL (JSON) database written in Rust with Python bindings. It supports a MongoDB-like query language, making it familiar and easy to use for developers experienced with document-based databases. Bison is designed to efficiently store, query, and update JSON data with support for various query operators.

## Features

- **NoSQL Document Storage**: Stores JSON documents in collections.
- **MongoDB-like Query Language**: Use familiar query operators such as `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` for filtering documents.
- **Insert and Query**: Easily insert documents into collections and retrieve them based on queries.
- **Update Operators**: Modify documents using `$set`, `$inc`, `$dec`, `$add`, `$substract`, and `$delete` operators.
- **Mixed Queries**: Perform complex queries with multiple conditions and nested fields.
- **Python Bindings**: Fully integrated with Python via bindings, allowing you to use Bison in Python projects.

## Installation

To use Bison in your Python project, install it using:

```bash
pip install bison-db
```

## Basic Usage

### Creating a Collection and Inserting Documents

```python
from bison import Bison

db = Bison()

# Create a collection
db.create_collection("test")

# Insert documents
db.insert("test", {"a": 10, "b": 20})
db.insert("test", {"a": True, "b": False})
```

### Querying Data

```python
# Simple equality query
result = db.find("test", {"a": 10})
print(result)  # Returns documents where field 'a' equals 10

# Query with greater than operator
result = db.find("test", {"a": {"$gt": 5}})
print(result)  # Returns documents where 'a' is greater than 5
```

### Update Documents

```python
# Update document by setting a new value
db.update("test", {"a": {"$set": 30}})

# Increment a field
db.update("test", {"a": {"$inc": ""}})

# Decrement a field
db.update("test", {"a": {"$dec": ""}})
```

### Delete Fields

```python
# Delete a field from a document
db.update("test", {"a": {"$delete": ""}})
```

## Query Operators

Bison supports a range of MongoDB-like query operators:

- `$eq`: Matches values that are equal to a specified value.

- `$ne`: Matches all values that are not equal to a specified value.

- `$gt`: Matches values that are greater than a specified value.

- `$gte`: Matches values that are greater than or equal to a specified value.

- `$lt`: Matches values that are less than a specified value.

- `$lte`: Matches values that are less than or equal to a specified value.

### Example Queries

```python
# Equality
result = db.find("test", {"a": {"$eq": 10}})

# Not equal
result = db.find("test", {"a": {"$ne": 20}})

# Greater than
result = db.find("test", {"a": {"$gt": 10}})

# Less than
result = db.find("test", {"a": {"$lt": 100}})
```

## Update Operators

Bison provides several operators for updating fields within documents:

- `$set`: Sets the value of a field.

- `$inc`: Increments a field by 1.

- `$dec`: Decrements a field by 1.

- `$add`: Adds a specified value to a field.

- `$substract`: Subtracts a specified value from a field.

- `$delete`: Deletes a field from a document.

### Example Updates

```python
# Set a value
db.update("test", {"a": {"$set": 40}})

# Increment a field
db.update("test", {"b": {"$inc": ""}})

# Delete a field
db.update("test", {"a": {"$delete": ""}})
```

## Mixed Queries

You can combine multiple query conditions, including nested fields:

```python
# Query with mixed conditions
result = db.find(
    "test",
    {
        "a": {"$eq": {"myobj": 20}},
        "b": {"$gt": 19},
        "c": {"$lte": 120}
    }
)
print(result)  # Returns documents matching all the conditions
```

## Handling Errors

Invalid queries will raise exceptions. For example:

```python
from bison import Bison
import pytest

db = Bison()

# Insert a document
db.insert("test", {"a": 10})

# Invalid query
with pytest.raises(ValueError):
    db.find("test", {"a": {"$gt": False}})
```
