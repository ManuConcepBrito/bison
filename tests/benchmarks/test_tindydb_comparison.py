import pytest
from tinydb import Query
import logging


logger = logging.getLogger(__name__)


@pytest.mark.comparison
@pytest.mark.parametrize(
    "pattern_name", ["50% Reads, 50% Updates", "95% Reads, 5% Updates"]
)
def test_bisondb_benchmark(
    benchmark, bisondb_benchmark, db_benchmark_operations, pattern_name
):
    db = bisondb_benchmark

    # Find the operations for the given pattern
    for pattern in db_benchmark_operations:
        if pattern["name"] == pattern_name:
            operations = pattern["operations"]
            break

    def run_operations():
        for op in operations:
            if op["op"] == "read":
                db.find(
                    "test",
                    {
                        "age": {"$gte": op["query"]["age"]["$gte"]},
                        "address": {"city": op["query"]["address.city"]},
                    },
                )
            else:
                db.update(
                    "test",
                    {
                        "balance": {"$set": op["update"]["balance"]["$set"]},
                        "address": {
                            "zip": {"$set": op["update"]["address.zip"]["$set"]}
                        },
                    },
                    {"user_id": {"$eq": op["filter"]["user_id"]}},
                )
                # fair comparison: tinydb writes to storage on every update
                db.write("test")

    benchmark(run_operations)


@pytest.mark.comparison
@pytest.mark.parametrize(
    "pattern_name", ["50% Reads, 50% Updates", "95% Reads, 5% Updates"]
)
def test_tinydb_benchmark(
    benchmark, tinydb_benchmark, db_benchmark_operations, pattern_name
):
    db = tinydb_benchmark
    User = Query()

    # Extract the operations sequence for TinyDB

    # Find the operations for the given pattern
    for pattern in db_benchmark_operations:
        if pattern["name"] == pattern_name:
            operations = pattern["operations"]
            break

    # Benchmark the intertwined operations
    def run_operations():
        for op in operations:
            if op["op"] == "read":
                db.search(
                    (User.age >= op["query"]["age"]["$gte"])
                    & (User["address"]["city"] == op["query"]["address.city"])
                )
            else:
                db.update(
                    {
                        "balance": op["update"]["balance"]["$set"],
                        "address": {"zip": op["update"]["address.zip"]["$set"]},
                    },
                    User.user_id == op["filter"]["user_id"],
                )

    # Run the benchmark
    benchmark(run_operations)
