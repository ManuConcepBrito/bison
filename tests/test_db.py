import os
import pytest
from bison import Bison
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def test_create_from_document(tmp_path: Path) -> None:
    json_data = {
        "name": "Test Project",
        "documents": {"1": [0, 1, 2, 3], "2": "string", "3": {"status": False}},
    }
    document_path = os.path.join(tmp_path, "document.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db = Bison("new_db", document_path)
    try:
        assert set(db.collections()) == set(json_data.keys())

        # Assert values are equal
        for key in json_data.keys():
            values = db.find(key)[0]
            assert values == json_data[key]
    finally:
        db.drop_all()


def test_insert_many(db: Bison) -> None:
    collection_name = "test"
    collection_data = []
    db.insert(collection_name, {"a": 10, "b": 200})
    for ii in range(10):
        collection_data.append({"a": ii, "b": 10 + ii})
    db.insert_many(collection_name, collection_data)
    assert len(db.find(collection_name, {})) == len(collection_data) + 1


def test_insert_many_from_document(db: Bison, tmp_path: Path) -> None:
    collection_name = "test"
    json_data = [{"a": 10, "b": 200}, {"a": 1, "b": 20}]

    document_path = os.path.join(tmp_path, "document.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db.insert_many_from_document(collection_name, document_path)
    assert len(db.find(collection_name, {})) == len(json_data)


def test_remove_collection(db: Bison) -> None:
    db.drop_collection("test")

    collections = db.collections()

    assert "test" not in collections


def test_missing_collection(db: Bison) -> None:
    with pytest.raises(ValueError):
        db.find("non_existing_collection", {"some_value": 10})


def test_find_on_existing_db(tmp_path: Path) -> None:
    """Data correctly loaded from existing db"""
    collection_name = "test"
    data_in_collection = [{"a": 10, "b": 200}, {"a": 1, "b": 20}]
    json_data = {f"{collection_name}": data_in_collection}

    document_path = os.path.join(tmp_path, f"{collection_name}.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db = Bison(str(tmp_path))

    found_in_db = db.find(collection_name)

    assert found_in_db == data_in_collection


def test_update_on_existing_db(tmp_path: Path) -> None:
    """Data correctly loaded from existing db"""
    collection_name = "test"
    data_in_collection = [{"a": 10, "b": 200}, {"a": 1, "b": 20}]
    updated_collection = [{"a": 11, "b": 200}, {"a": 2, "b": 20}]
    json_data = {f"{collection_name}": data_in_collection}

    document_path = os.path.join(tmp_path, f"{collection_name}.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db = Bison(str(tmp_path))

    found_in_db = db.update(collection_name, {"a": {"$inc": ""}}, return_result=True)

    assert found_in_db == updated_collection
