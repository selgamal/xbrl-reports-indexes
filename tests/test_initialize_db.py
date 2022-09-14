"""Test database initialization"""
from __future__ import annotations

import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import Session
from xbrlreportsindexes.core import index_db

from .const import test_data_dir


db = index_db.XbrlIndexDB.make_test_db(test_data_dir=test_data_dir())


def test_all_tables_created() -> None:
    """Check if all tables where actually created"""
    db_tables = set(inspect(db.engine).get_table_names())
    model_tables = set(db.metadata.tables.keys())
    assert db_tables == model_tables


@pytest.mark.parametrize(
    "table, count_rows",
    [
        ("sec_industry_structure", 13),
        ("sp_companies_ciks", 500),
        ("sec_cik_ticker_mapping", 12042),
        ("sec_industry_level", 12454),
        ("sec_industry", 4333),
    ],
)
def test_count_rows(table: str, count_rows: int) -> None:
    """Compare rows count to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = session.query(db.metadata.tables[table]).count()
    assert db_rows == count_rows
