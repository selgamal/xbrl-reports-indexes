"""Test ESEF index after first load"""
from __future__ import annotations

import pathlib
from typing import Any

import pytest
from sqlalchemy.orm import Session
from xbrlreportsindexes.core import index_db
from xbrlreportsindexes.model import ESEF

from .const import test_data_dir


db = index_db.XbrlIndexDB.make_test_db(test_data_dir=test_data_dir())
assert isinstance(db._db_mock_test_data_dir, pathlib.Path)
db.update_esef_default(
    loc=db._db_mock_test_data_dir.joinpath(
        "esef", "index", "index_01.json"
    ).as_uri()
)


@pytest.mark.parametrize(
    "table, count_rows",
    [
        (ESEF.EsefFiling, 8),
        (ESEF.EsefFilingError, 26),
        (ESEF.EsefFilingLang, 0),
        (ESEF.EsefEntity, 6),
        (ESEF.EsefEntityOtherName, 1),
        (ESEF.EsefInferredFilingLanguage, 8),
    ],
)
def test_count_rows(table: Any, count_rows: int) -> None:
    """Compare rows count to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = session.query(table).count()
    assert db_rows == count_rows
