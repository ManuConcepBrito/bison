import pytest
from typing import Generator, Any
from pathlib import Path
from bison import Bison
from tinydb import TinyDB
import os

@pytest.fixture(scope="function")
def db(tmp_path: Path) -> Generator[Any, Any, Bison]:
    db = Bison("test")
    yield db
    db.drop_all()


@pytest.fixture(scope="function")
def tinydb_benchmark(tmp_path: Path):
    # Setup: Create a TinyDB instance and populate it with 1000 elements
    db_path = os.path.join(tmp_path, "db.json")
    db = TinyDB(db_path)
    table = db.table("elements")
    # Create 1000 elements with 5 fields
    for i in range(1000):
        table.insert(
            {
                "id": i,
                "name": f"Element {i}",
                "location": f"Location {i}",
                "value": i * 10,
                "status": "active" if i % 2 == 0 else "inactive",
            }
        )

    yield db  # This allows the test to use the db

    # Teardown: Cleanup the database file after tests
    db.drop_tables()
    db.close()


@pytest.fixture(scope="function")
def bisondb_benchmark(tmp_path: Path):
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
    db.insert_many("elements", elements_to_insert)
    yield db  # This allows the test to use the db

    # Teardown: Cleanup the database file after tests
    db.drop_all()


# pylint: disable=unused-argument
def pytest_benchmark_scale_unit(
    config: Any, unit: Any, benchmarks: Any, best: Any, worst: Any, sort: Any
) -> Any:
    """This scales the benchmkar units to milliseconds"""
    prefix = ""
    scale = 1
    if unit == "seconds":
        prefix = "millisec"
        scale = 1000
    return prefix, scale

