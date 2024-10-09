import os
import pytest
from bison import Bison
import json
from pathlib import Path


def test_create_from_document(tmp_path: Path) -> None:
    json_data = {
        "name": "Test Project",
        "documents": {"1": [0, 1, 2, 3], "2": "string", "3": {"status": False}},
    }
    document_path = os.path.join(tmp_path, "document.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db = Bison("new_db", document_path)

    assert set(db.collections()) == set(json_data.keys())

    # Assert values are equal
    for key in json_data.keys():
        values = db.find(key)[0]
        assert values == json_data[key]


def test_insert_many(db: Bison) -> None:
    collection_name = "test"
    collection_data = []
    db.insert(collection_name, {"a": 10, "b": 200})
    for ii in range(10):
        collection_data.append({"a": ii, "b": 10 + ii})
    db.insert_many(collection_name, collection_data)
    assert len(db.find(collection_name, {})) == len(collection_data) + 1
