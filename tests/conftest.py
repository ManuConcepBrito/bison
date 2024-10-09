import pytest
from typing import Generator, Any
from pathlib import Path
from bison import Bison


@pytest.fixture(scope="function")
def db(tmp_path: Path) -> Generator[Any, Any, Bison]:

    db = Bison("test")
    yield db
    db.drop_all()
