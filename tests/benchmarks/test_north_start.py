from bison import Bison
from typing import Callable
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def test_insert(benchmark, tmp_path: Path):
    db = Bison(str(tmp_path))
    # Create 1000 elements with 5 fields
    elements_to_insert = []
    for i in range(1000):
        elements_to_insert.append(
            {
                "id": i,
                "name": f"Element {i}",
                "location": f"Location {i}",
                "value": i * 10,
                "status": "active" if i % 2 == 0 else "inactive",
            }
        )

    def insert_and_flush():
        db.insert_many("elements", elements_to_insert)
        db.write_all()
    benchmark(insert_and_flush)


def test_find_no_cache(bisondb_benchmark: Bison, benchmark: Callable[..., None]) -> None:
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
