from bison import Bison
from typing import Callable
from tinydb import Query
from tinydb.operations import set
import logging

logger = logging.getLogger(__name__)


# def test_find_tinydb(tinydb_benchmark, benchmark):
#     query = Query()
#     table = tinydb_benchmark.table("elements")
#
#     result = benchmark(table.search, query.location == "Location 500")
#     assert result[0] == {
#         "id": 500,
#         "name": "Element 500",
#         "location": "Location 500",
#         "value": 5000,
#         "status": "active",
#     }
#
#
# def test_find(bisondb_benchmark: Bison, benchmark: Callable[..., None]) -> None:
#     result = benchmark(
#         bisondb_benchmark.find, "elements", {"location": "Location 500"}
#     )
#
#     assert result[0] == {
#         "id": 500,
#         "name": "Element 500",
#         "location": "Location 500",
#         "value": 5000,
#         "status": "active",
#     }
#


def test_find_tinydb_no_cache(tinydb_benchmark, benchmark):
    query = Query()
    table = tinydb_benchmark.table("elements")

    def search_no_cache():
        table.clear_cache()
        result = table.search(query.location == "Location 500")
        return result

    result = benchmark(search_no_cache)
    assert result[0] == {
        "id": 500,
        "name": "Element 500",
        "location": "Location 500",
        "value": 5000,
        "status": "active",
    }


def test_find(bisondb_benchmark: Bison, benchmark: Callable[..., None]) -> None:
    def search_no_cache():
        bisondb_benchmark.clear_cache()
        return bisondb_benchmark.find("elements", {"location": "Location 500"})

    result = benchmark(search_no_cache)

    assert result[0] == {
        "id": 500,
        "name": "Element 500",
        "location": "Location 500",
        "value": 5000,
        "status": "active",
    }


def test_update_tinydb(tinydb_benchmark, benchmark: Callable[..., None]) -> None:
    table = tinydb_benchmark.table("elements")
    benchmark(table.update, set("value", 10))
    # Assert correctness of operation
    query = Query()
    docs_with_value = table.search(query.value.exists())

    assert all(doc["value"] == 10 for doc in docs_with_value)


def test_update(bisondb_benchmark: Bison, benchmark: Callable[..., None]) -> None:
    updated_db = benchmark(
        bisondb_benchmark.update, "elements", {"value": {"$set": 10}}
    )
    for elem in updated_db:
        assert elem["value"] == 10


def test_update_and_flush(bisondb_benchmark, benchmark: Callable[..., None]) -> None:
    def update_and_flush() -> None:
        bisondb_benchmark.update("elements", {"value": {"$set": 10}})
        bisondb_benchmark.write_all()

    benchmark(update_and_flush)
