import pytest
from typing import Dict, Any
from bison import Bison
import logging


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "insert, query",
    [
        ({"a": 10, "b": 20}, {"a": 10}),
        ({"a": True, "b": False}, {"b": False}),
        ({"a": "my_name", "b": 20, "c": False}, {"a": "my_name"}),
    ],
)
def test_simple_eq_query(
    db: Bison, insert: Dict[str, Any], query: Dict[str, Any]
) -> None:
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, insert)
    query_result = db.find(collection_name, query)
    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == insert


@pytest.mark.parametrize(
    "insert, filter_query",
    [
        ({"a": 10, "b": 20}, {"a": {"$eq": 10}}),
        ({"a": True, "b": {"c": 10}}, {"b": {"$eq": {"c": 10}}}),
    ],
)
def test_eq_query(
    db: Bison,
    insert: Dict[str, Any],
    filter_query: Dict[str, Any],
) -> None:
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, insert)
    query_result = db.find(collection_name, filter_query)
    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == insert


@pytest.mark.parametrize(
    "insert, filter_query",
    [
        ({"a": 10, "b": 20}, {"a": {"$ne": 20}}),
        ({"a": True, "b": {"c": 10}}, {"b": {"$ne": {"d": 10}}}),
        ({"a": True, "b": {"c": 10}}, {"b": {"$ne": {"c": 20}}}),
    ],
)
def test_ne_query(
    db: Bison,
    insert: Dict[str, Any],
    filter_query: Dict[str, Any],
) -> None:
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, insert)
    query_result = db.find(collection_name, filter_query)
    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == insert


def test_gt_query(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, {"a": 20})
    db.insert(collection_name, {"a": 100})
    db.insert(collection_name, {"a": 101})
    query_result = db.find(collection_name, {"a": {"$gt": 100}})

    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == {"a": 101}


def test_gte_query(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, {"a": 20})
    db.insert(collection_name, {"a": 100})
    db.insert(collection_name, {"a": 101})
    query_result = db.find(collection_name, {"a": {"$gte": 101}})

    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == {"a": 101}


def test_lt_query(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, {"a": 20})
    db.insert(collection_name, {"a": 100})
    db.insert(collection_name, {"a": 101})
    query_result = db.find(collection_name, {"a": {"$lt": 100}})

    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == {"a": 20}


def test_lte_query(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, {"a": 20})
    db.insert(collection_name, {"a": 100})
    db.insert(collection_name, {"a": 101})
    query_result = db.find(collection_name, {"a": {"$lte": 20}})

    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    assert query_result == {"a": 20}


def test_mixed_queries(db: Bison):
    collection_name = "test"
    insert_value = {"a": {"myobj": 20}, "b": 20, "c": 120}
    db.insert(collection_name, insert_value)

    query_result = db.find(
        "test", {"a": {"$eq": {"myobj": 20}},
                 "b": {"$gt": 19}, "c": {"$lte": 120}}
    )
    assert len(query_result) == 1
    query_result = query_result[0]
    assert query_result == insert_value


@pytest.mark.parametrize(
    "initial_value, update_query, updated_value",
    [
        ({"a": 10}, {"a": 30}, {"a": 30}),
        ({"a": 20}, {"a": {"$set": 40}}, {"a": 40}),
        ({"a": {"b": 20}}, {"a": {"b": {"$set": 40}}}, {"a": {"b": 40}}),
        ({"a": {"b": 20}}, {"a": {"$set": {"c": 30}}}, {"a": {"c": 30}}),
    ],
)
def test_simple_set_update(
    db: Bison,
    initial_value: Dict[str, Any],
    update_query: Dict[str, Any],
    updated_value: Dict[str, Any],
) -> None:
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, initial_value)

    updated_db = db.update(collection_name, update_query, return_result=True)
    assert updated_db[0] == updated_value

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write == updated_db


def test_increment(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)

    insert_value = {"a": {"myobj": 20}, "b": 20, "c": {"d": 100}}
    db.insert(collection_name, insert_value)

    updated_db = db.update(
        collection_name, {"b": {"$inc": ""}}, return_result=True)

    assert updated_db[0]["b"] == insert_value["b"] + 1

    updated_db = db.update(
        collection_name, {"c": {"d": {"$inc": ""}}}, return_result=True
    )

    assert updated_db[0]["c"]["d"] == insert_value["c"]["d"] + 1

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write == updated_db


def test_decrement(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)

    insert_value = {"a": {"myobj": 20}, "b": 20, "c": {"d": 100}}
    db.insert(collection_name, insert_value)

    updated_db = db.update(
        collection_name, {"b": {"$dec": ""}}, return_result=True)

    # Assert changes in file
    assert updated_db[0]["b"] == insert_value["b"] - 1

    updated_db = db.update(
        collection_name, {"c": {"d": {"$dec": ""}}}, return_result=True
    )

    # Assert changes in file
    assert updated_db[0]["c"]["d"] == insert_value["c"]["d"] - 1

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write == updated_db


def test_add(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    add_value = 5
    insert_value = {"a": {"myobj": 20}, "b": 20, "c": {"d": 100}}
    db.insert(collection_name, insert_value)

    updated_db = db.update(
        collection_name, {"b": {"$add": add_value}}, return_result=True
    )

    # Assert changes in file
    assert updated_db[0]["b"] == insert_value["b"] + add_value

    updated_db = db.update(
        collection_name, {"c": {"d": {"$add": add_value}}}, return_result=True
    )

    # Assert changes in file
    assert updated_db[0]["c"]["d"] == insert_value["c"]["d"] + add_value

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write == updated_db


def test_substract(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    substract_value = 5
    insert_value = {"a": {"myobj": 20}, "b": 20, "c": {"d": 100}}
    db.insert(collection_name, insert_value)

    updated_db = db.update(
        collection_name, {"b": {"$substract": substract_value}}, return_result=True
    )

    # Assert changes in file
    assert updated_db[0]["b"] == insert_value["b"] - substract_value

    updated_db = db.update(
        collection_name,
        {"c": {"d": {"$substract": substract_value}}},
        return_result=True,
    )

    # Assert changes in file
    assert updated_db[0]["c"]["d"] == insert_value["c"]["d"] - substract_value

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write == updated_db


def test_delete(db: Bison):
    collection_name = "test"
    db.create_collection(collection_name)
    insert_value = {"a": {"myobj": 20, "another_obj": 30},
                    "b": 20, "c": {"d": 100}}
    db.insert(collection_name, insert_value)

    updated_db = db.update(
        collection_name, {"b": {"$delete": ""}}, return_result=True)

    # Assert changes in file
    after_write = db.find(collection_name, {})
    assert after_write == updated_db

    assert len(updated_db) == 1
    updated_db = updated_db[0]
    assert "b" not in updated_db

    updated_db = db.update(
        collection_name, {"a": {"myobj": {"$delete": ""}}}, return_result=True
    )

    # Assert changes in file
    assert len(updated_db) == 1
    updated_db = updated_db[0]
    assert "myobj" not in updated_db["a"]

    db.write_all()
    after_write = db.find(collection_name, {})
    assert after_write[0] == updated_db


@pytest.mark.parametrize(
    "query",
    [
        ({"a": {"$gt": False}}),
        ({"b": {"$gte": []}}),
        ({"c": {"$lt": False}}),
        ({"d": {"$lte": False}}),
    ],
)
def test_not_valid_query(db: Bison, query: Dict[str, Any]) -> None:
    db.insert("test", {"a": 10})
    with pytest.raises(ValueError):
        db.find("test", {"a": {"$gt": False}})
