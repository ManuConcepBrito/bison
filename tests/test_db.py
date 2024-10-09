import os
import pytest
from bison import Bison
import json
from pathlib import Path


def test_create_from_document(tmp_path: Path) -> None:
    json_data = {
        "name": "Test Project",
        "documents": {"1": [0, 1, 2, 3], "2": "string", "3": {"status": False}},
    }
    document_path = os.path.join(tmp_path, "document.json")
    with open(document_path, "w") as f:
        json.dump(json_data, f)
    db = Bison("new_db", document_path)

    assert set(db.collections()) == set(json_data.keys())
    db.drop_all()
