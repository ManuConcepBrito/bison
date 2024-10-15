import pytest
import random
import string
from typing import Generator, Any
from pathlib import Path
from bison import Bison
from tinydb import TinyDB

NUM_DOCUMENTS = 100
NUM_OPERATIONS = 100
random.seed(42)


@pytest.fixture(scope="function")
def db(tmp_path: Path) -> Generator[Any, Any, Bison]:
    db = Bison(str(tmp_path))
    yield db
    db.drop_all()


@pytest.fixture(scope="function")
def tinydb_benchmark(tmp_path, benchmark_dataset):
    db_path = tmp_path / "tinydb.json"
    db = TinyDB(db_path)
    db.insert_multiple(benchmark_dataset)
    yield db
    db.drop_tables()


@pytest.fixture(scope="function")
def bisondb_benchmark(tmp_path, benchmark_dataset):
    db = Bison(str(tmp_path))
    db.create_collection("test")
    db.insert_many("test", benchmark_dataset)
    yield db
    db.drop_all()


@pytest.fixture(scope="session")
def benchmark_dataset():

    def generate_document():
        return {
            "user_id": "".join(
                random.choices(string.ascii_letters + string.digits, k=12)
            ),
            "name": {
                "first": "".join(random.choices(string.ascii_letters, k=5)),
                "last": "".join(random.choices(string.ascii_letters, k=7)),
            },
            "age": random.randint(18, 90),
            "email": "".join(random.choices(string.ascii_letters, k=7))
            + "@example.com",
            "balance": random.uniform(1000, 5000),
            "address": {
                "street": "".join(
                    random.choices(string.ascii_letters +
                                   string.digits + " ", k=15)
                ),
                "city": "".join(random.choices(string.ascii_letters, k=10)),
                "zip": "".join(random.choices(string.digits, k=5)),
            },
            "orders": [
                {
                    "order_id": "".join(
                        random.choices(string.ascii_letters +
                                       string.digits, k=8)
                    ),
                    "amount": random.uniform(10, 500),
                    "date": f"{random.randint(2018, 2021)}-{random.randint(1,12):02}-{random.randint(1,28):02}",
                }
                for _ in range(random.randint(1, 5))
            ],
        }

    dataset = [generate_document() for _ in range(NUM_DOCUMENTS)]
    return dataset


@pytest.fixture(scope="session")
def db_benchmark_operations(benchmark_dataset):
    user_ids = [doc["user_id"] for doc in benchmark_dataset]

    def generate_db_operations_sequence(
        read_ratio, update_ratio, num_operations, user_ids
    ):
        read_operations = int(num_operations * read_ratio)
        update_operations = int(num_operations * update_ratio)

        operations = []

        for _ in range(read_operations):
            age = random.randint(18, 90)
            city = "".join(random.choices(string.ascii_letters, k=10))
            operations.append(
                {
                    "op": "read",
                    "query": {
                        "age": {"$gte": age},
                        "address.city": city,
                    },
                }
            )

        for _ in range(update_operations):
            user_id = random.choice(user_ids)
            new_balance = random.uniform(1000, 5000)
            operations.append(
                {
                    "op": "update",
                    "filter": {"user_id": user_id},
                    "update": {
                        "balance": {"$set": new_balance},
                        "address.zip": {
                            "$set": "".join(random.choices(string.digits, k=5))
                        },
                    },
                }
            )

        # Shuffle operations
        random.shuffle(operations)
        return operations

    usage_patterns = []

    for pattern in [
        {"name": "50% Reads, 50% Updates", "read_ratio": 0.5, "update_ratio": 0.5},
        {"name": "95% Reads, 5% Updates", "read_ratio": 0.95, "update_ratio": 0.05},
    ]:
        operations = generate_db_operations_sequence(
            pattern["read_ratio"], pattern["update_ratio"], NUM_OPERATIONS, user_ids
        )
        pattern["operations"] = operations
        usage_patterns.append(pattern)

    return usage_patterns


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
