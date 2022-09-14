"""Test ESEF index after second load"""
from __future__ import annotations

import pathlib
import pickle
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
db.update_esef_default(
    loc=db._db_mock_test_data_dir.joinpath(
        "esef", "index", "index_02.json"
    ).as_uri()
)

test_data_src: dict[str, list[dict[str, Any]]] = {}
test_data_file = db._db_mock_test_data_dir.joinpath(
    "pickles", "esef_test_data.pkl"
)
with open(test_data_file, "rb") as pkl:
    test_data_src = pickle.load(pkl)


@pytest.mark.parametrize(
    "table, count_rows",
    [
        (ESEF.EsefFiling, 10),
        (ESEF.EsefFilingError, 32),
        (ESEF.EsefFilingLang, 0),
        (ESEF.EsefEntity, 7),
        (ESEF.EsefEntityOtherName, 1),
        (ESEF.EsefInferredFilingLanguage, 10),
    ],
)
def test_count_rows(table: Any, count_rows: int) -> None:
    """Compare rows count to expected"""
    db_rows = 0
    with Session(db.engine) as session:
        db_rows = session.query(table).count()
    assert db_rows == count_rows


@pytest.mark.parametrize(
    "table, sort_cols",
    [
        (ESEF.EsefFiling, (ESEF.EsefFiling.filing_key,)),
        (ESEF.EsefFilingError, (ESEF.EsefFilingError.filing_id,)),
        (ESEF.EsefFilingLang, (ESEF.EsefFilingLang.filing_id,)),
        (ESEF.EsefEntity, (ESEF.EsefEntity.entity_lei,)),
        (
            ESEF.EsefEntityOtherName,
            (
                ESEF.EsefEntityOtherName.entity_lei,
                ESEF.EsefEntityOtherName.other_name,
            ),
        ),
        (
            ESEF.EsefInferredFilingLanguage,
            (
                ESEF.EsefInferredFilingLanguage.filing_id,
                ESEF.EsefInferredFilingLanguage.lang,
            ),
        ),
    ],
)
def test_esef_data(table: Any, sort_cols: Any) -> None:
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
        (
            {"filer_name": "direct"},
            {"213800FF2R23ALJQOP04/2021-12-31/ESEF/GB/0"},
        ),
        (
            {
                "publication_date_from": "2021-01-01",
                "publication_date_to": "2021-12-31",
            },
            {
                "7CUNS533WID6K7DGFI87/2020-12-31/ESEF/ES/0",
                "259400NAPDFBOTNCRL54/2020-12-31/ESEF/PL/0",
                "25940037UC4MNP02D242/2020-12-31/ESEF/PL/0",
            },
        ),
    ],
)
def test_search(search_params: dict[str, str], result: set[str]) -> None:
    """Compare search results to expected"""
    q = db.search_filings("esef", **search_params)  # type: ignore[arg-type]
    res = set()
    with Session(db.engine) as session:
        res = {x.filing_key for x in q.with_session(session)}
    assert res == result
