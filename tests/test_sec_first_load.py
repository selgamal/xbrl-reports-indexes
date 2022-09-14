"""Test SEC feeds after first load"""
from __future__ import annotations

import pathlib
from typing import Any

import pytest
from sqlalchemy.orm import Session
from xbrlreportsindexes.core import index_db
from xbrlreportsindexes.model import SEC

from .const import test_data_dir


db = index_db.XbrlIndexDB.make_test_db(test_data_dir=test_data_dir())
assert isinstance(db._db_mock_test_data_dir, pathlib.Path)
db.update_sec_default(
    loc=db._db_mock_test_data_dir.joinpath(
        "sec", "monthly", "monthly_01.html"
    ).as_uri()
)


@pytest.mark.parametrize(
    "table, count_rows",
    [
        (SEC.SecFeed, 1),
        (SEC.SecFiling, 2),
        (SEC.SecFile, 28),
        (SEC.SecFiler, 2),
        (SEC.SecFormerNames, 1),
    ],
)
def test_count_rows(table: Any, count_rows: int) -> None:
    """Compare rows count to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = session.query(table).count()
    assert db_rows == count_rows


def test_count_duplicates() -> None:
    """Compare duplicates count to expected"""
    db_rows = 1
    with Session(db.engine) as session:
        db_rows = (
            session.query(SEC.SecFiling)
            .filter(SEC.SecFiling.duplicate == 1)
            .count()
        )
    assert db_rows == 0
