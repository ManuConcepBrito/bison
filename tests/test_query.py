import pytest
from typing import Dict, Any
from bison import Bison
import logging


logger = logging.getLogger(__name__)


@pytest.mark.parametrize("insert, query", [(
                {"a": 10, "b": 20}, {"a": 10}),
                ({"a": True, "b": False}, {"b": False}),
                ({"a": "my_name", "b": 20, "c": False}, {"a": "my_name"})])
def test_simple_query(db: Bison, insert: Dict[str, Any], query: Dict[str, Any]) -> None:
    collection_name = "test"
    db.create_collection(collection_name)
    db.insert(collection_name, insert)
    query_result = db.find(collection_name, query)
    assert len(query_result) == 1
    # pop id
    query_result = query_result[0]
    query_result.pop("_id")
    assert query_result == insert


def test_remove_collection(db: Bison) -> None:
    db.drop_collection("test")

    collections = db.collections()

    assert "test" not in collections
