"""Test SEC feeds after third load"""
from __future__ import annotations

import pathlib
import pickle
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
db.update_sec_default(
    loc=db._db_mock_test_data_dir.joinpath(
        "sec", "monthly", "monthly_02.html"
    ).as_uri()
)
db.update_sec_default(
    loc=db._db_mock_test_data_dir.joinpath(
        "sec", "monthly", "monthly_03.html"
    ).as_uri()
)

test_data_src: dict[str, list[dict[str, Any]]] = {}
test_data_file = db._db_mock_test_data_dir.joinpath(
    "pickles", "sec_test_data.pkl"
)
with open(test_data_file, "rb") as pkl:
    test_data_src = pickle.load(pkl)


@pytest.mark.parametrize(
    "table, count_rows",
    [
        (SEC.SecFeed, 3),
        (SEC.SecFiling, 7),
        (SEC.SecFile, 87),
        (SEC.SecFiler, 6),
        (SEC.SecFormerNames, 3),
    ],
)
def test_count_rows(table: Any, count_rows: int) -> None:
    """Compare row counts to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = session.query(table).count()
    assert db_rows == count_rows


def test_count_duplicates() -> None:
    """Compare duplicates counts to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = (
            session.query(SEC.SecFiling)
            .filter(SEC.SecFiling.duplicate == 1)
            .count()
        )
    assert db_rows == 1


@pytest.mark.parametrize(
    "table, sort_cols",
    [
        (SEC.SecFeed, (SEC.SecFeed.feed_id,)),
        (SEC.SecFiling, (SEC.SecFiling.accession_number,)),
        (SEC.SecFile, (SEC.SecFile.accession_number, SEC.SecFile.sequence)),
        (SEC.SecFiler, (SEC.SecFiler.cik_number,)),
        (
            SEC.SecFormerNames,
            (
                SEC.SecFormerNames.cik_number,
                SEC.SecFormerNames.date_changed,
            ),
        ),
    ],
)
def test_sec_data(table: Any, sort_cols: tuple[Any]) -> None:
    """Compare tables data to expected"""
    src_data = test_data_src[table.__tablename__]
    table_rows = []
    with Session(db.engine) as session:
        table_data = session.query(table).order_by(*sort_cols)
        for row in table_data:
            _row = row.to_dict()
            _row.pop("created_updated_at", None)
            table_rows.append(_row)
    assert table_rows == src_data


@pytest.mark.parametrize(
    "search_params, result",
    [
        ({"filer_name": "permian"}, {"0001193125-22-241104"}),
        (
            {
                "publication_date_from": "2022-08-01",
                "publication_date_to": "2022-08-31",
            },
            {"0001493152-22-024774", "0001140361-22-031812"},
        ),
    ],
)
def test_search(search_params: dict[str, str], result: set[str]) -> None:
    """Compare search results to expected"""
    q = db.search_filings("sec", **search_params)  # type: ignore[arg-type]
    res = set()
    with Session(db.engine) as session:
        res = {x.accession_number for x in q.with_session(session)}
    assert res == result
