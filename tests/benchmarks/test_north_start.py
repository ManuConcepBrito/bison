from bison import Bison
import pytest
import random
import string
from typing import Callable
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@pytest.mark.north_start
def test_load_database(benchmark, tmp_path: Path, benchmark_dataset) -> None:
    # Create database in tmp_path
    db = Bison(str(tmp_path))
    db.create_collection("test")
    db.insert_many("test", benchmark_dataset)
    db.write_all()
    del db

    def load_database():
        Bison(str(tmp_path))

    benchmark(load_database)


@pytest.mark.north_start
def test_insert_many(
    benchmark, tmp_path: Path, benchmark_dataset
) -> None:
    db = Bison(str(tmp_path))

    def insert_and_flush():
        db.insert_many("test", benchmark_dataset)
        db.write_all()

    benchmark(insert_and_flush)


@pytest.mark.north_start
def test_find_no_cache(
    bisondb_benchmark: Bison, benchmark: Callable[..., None]
) -> None:
    def search_no_cache():
        age = random.randint(18, 90)

        city = "".join(random.choices(string.ascii_letters, k=10))
        bisondb_benchmark.clear_cache()
        return bisondb_benchmark.find(
            "test", {"age": f"{age}", "address.city": f"{city}"}
        )

    benchmark(search_no_cache)


@pytest.mark.north_start
def test_update(bisondb_benchmark: Bison, benchmark: Callable[..., None]) -> None:
    benchmark(
        bisondb_benchmark.update,
        "test",
        {
            "balance": {"$set": random.uniform(1000, 5000)},
            "address.zip": "".join(random.choices(string.digits, k=5)),
        },
    )


@pytest.mark.north_start
def test_update_and_flush(bisondb_benchmark, benchmark: Callable[..., None]) -> None:
    def update_and_flush() -> None:
        bisondb_benchmark.update(
            "test",
            {
                "balance": {"$set": random.uniform(1000, 5000)},
                "address.zip": "".join(random.choices(string.digits, k=5)),
            },
        )
        bisondb_benchmark.write_all()

    benchmark(update_and_flush)
