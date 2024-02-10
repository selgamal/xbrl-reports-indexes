"""Utilities to interact with the database"""
from __future__ import annotations

import datetime
import gettext
import logging
import os
import pickle
import sys
import time
from collections import defaultdict
from typing import Any
from typing import cast
from typing import Literal

from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import MetaData
from sqlalchemy import update
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from xbrlreportsindexes.core import constants
from xbrlreportsindexes.core.arelle_utils import CntlrPy
from xbrlreportsindexes.core.arelle_utils import IndexDBLogHandler
from xbrlreportsindexes.core.arelle_utils import RSS_DB_LOG_HANDLER_NAME
from xbrlreportsindexes.core.constants import log_template
from xbrlreportsindexes.core.constants import XIDBException
from xbrlreportsindexes.core.data_utils import chunks
from xbrlreportsindexes.core.data_utils import date_filter_feeds
from xbrlreportsindexes.core.data_utils import get_feed_info
from xbrlreportsindexes.core.data_utils import get_filer_information
from xbrlreportsindexes.core.data_utils import get_files_info
from xbrlreportsindexes.core.data_utils import get_filing_info
from xbrlreportsindexes.core.data_utils import get_monthly_rss_feeds_links
from xbrlreportsindexes.core.data_utils import get_new_and_modified_feeds
from xbrlreportsindexes.core.data_utils import get_new_or_modified_filings
from xbrlreportsindexes.core.data_utils import get_sec_cik_ticker_mapping
from xbrlreportsindexes.core.data_utils import get_index_companies_ciks
from xbrlreportsindexes.core.data_utils import get_time_elapsed
from xbrlreportsindexes.core.data_utils import ts_now
from xbrlreportsindexes.model import BASE
from xbrlreportsindexes.model import BASE_M
from xbrlreportsindexes.model import SEC


try:
    from arelle.Cntlr import Cntlr
    from arelle.FileSource import FileSource
    from arelle.ModelXbrl import ModelXbrl
    from arelle.ModelRssItem import ModelRssItem
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc


updatable_tables_functions = {
    getattr(
        SEC.SecCikTickerMapping, "__tablename__"
    ): get_sec_cik_ticker_mapping,
    getattr(SEC.SpCompaniesCiks, "__tablename__"): get_index_companies_ciks,
}


def create_connection_args(
    user: str | None,
    password: str | None,
    host: str | None,
    port: int | None,
    database: str,
    product: Literal["sqlite", "postgres", "mysql"],
    timeout: int | None = 60,
) -> dict[str, dict[str, Any]]:
    """Create sqlalchemy engine connection string, accommodating
    multiple backends"""
    if any(x is None for x in [database, product]):
        raise XIDBException(
            constants.ERR_BAD_CONN_PARAM,
            "At least database and product must be provided.",
        )
    connection_strings: dict[str, dict[str, Any]] = {
        "sqlite": {
            "url": f"sqlite:///{database}",
            "connect_args": {"timeout": timeout},
        },
        "postgres": {
            "url": f"postgresql+psycopg2://"  # pg8000
            f"{user}:{password}@{host}:{port or 5432}/{database}",
            "pool_timeout": timeout,
        },
        "mysql": {
            "url": f"mysql+pymysql:"
            f"//{user}:{password}@{host}:{port or 3306}/{database}",
            "pool_timeout": timeout,
        },
        # "mssql": {
        #     "url": f"mssql+pymssql:"
        #            f"//{user}:{password}@{host}:{port or 1433}/{database}",
        #     "pool_timeout": timeout,
        # },
        # "oracle": {
        #     "url": f"oracle+pymssql://"
        #            f"{user}:{password}@{host}:{port or 1521}/{database}",
        #     "pool_timeout": timeout,
        # },
    }
    return connection_strings[product]


def create_connection_engine(
    user: str | None,
    password: str | None,
    host: str | None,
    port: int | None,
    database: str,
    product: Literal["sqlite", "postgres", "mysql"],
    connection_string: str | None = None,
    timeout: int | None = 60,
) -> Engine:
    """Create connection engine based on product"""
    _connection_str: str | dict[str, Any] | None = connection_string
    if _connection_str is None:
        connection_str = create_connection_args(
            user, password, host, port, database, product, timeout
        )
    engine: Engine = create_engine(
        **connection_str, future=True
    )  # type: ignore[call-overload]
    return engine


def insert_or_delete_rows(
    conn: Connection,
    action: str,
    table_name: str,
    data: list[dict[str, Any]] | dict[str, Any] | None = None,
    commit: bool = False,
) -> int:
    """Helper function to insert or delete table contents
    `action` should be on of `insert` or `delete`.
    """
    rowcount = 0
    if action not in ("insert", "delete"):
        raise XIDBException(constants.ERR_UNKNOWN_ACTION)
    if action == "insert" and data is None:
        raise XIDBException(constants.ERR_MISSING_DATA)
    current_table = BASE.metadata.tables[table_name]
    stmts: list[delete | insert] = []
    if action == "insert":
        if isinstance(data, dict):
            stmts.append(insert(current_table).values(**data))
        elif isinstance(data, list):
            chunked = chunks(data, 100)
            for chunk in chunked:
                stmts.append(insert(current_table).values(chunk))
    elif action == "delete":
        stmts.append(delete(current_table))

    for stmt in stmts:
        cur = conn.execute(stmt)
        rowcount += cur.rowcount
    if commit:
        conn.commit()  # type: ignore[attr-defined]
    return rowcount


def load_rss_feed(
    link: str, cntlr: Cntlr | None = None, reload_cache: bool = True
) -> ModelXbrl:
    """Uses arelle Cntlr to load rss feed from the given link"""
    if cntlr is None:
        cntlr = CntlrPy()
    # account for multiple processes trying to create same
    # cache folder when cache is cleared
    while True:
        try:
            gettext.install("arelle")
            # always reload cache for modified feeds otherwise
            # reload when reloadCache is specified
            cntlr.webCache.getfilename(link, reload=reload_cache)
        except FileExistsError:
            time.sleep(0.5)
            continue
        break
    modelXbrl: ModelXbrl | None = None
    if sys.platform == "win32" and link.startswith("file:///"):
        link = link.replace("file:///", "")
    _fs = FileSource(link, cntlr)

    # try to load for 20 secs
    start_loop_time = time.perf_counter()
    while not modelXbrl:
        try:
            modelXbrl = cntlr.modelManager.load(_fs, "getting feed data")
        except Exception:
            pass
        time_taken = get_time_elapsed(start_loop_time)
        if time_taken >= 20 and modelXbrl is None:
            break
    cntlr.showStatus((f"Loaded rss feed and getting feed data from {link}"))
    assert isinstance(modelXbrl, ModelXbrl)
    return modelXbrl


def refresh_table_data(
    engine: Engine,
    cntlr: Cntlr,
    table_name: str,
    new_data: list[dict[str, Any]] | dict[str, Any],
) -> None:
    """Deletes existing rows and inserts new data"""
    inserted_rows = deleted_rows = 0
    del_time_taken = insert_time_taken = 0.0
    cntlr.addToLog(
        f"Refreshing table {table_name}.",
        **log_template("info", engine.url.database),
    )
    with engine.connect() as conn:
        try:
            # clean up first
            del_start_at = time.perf_counter()
            deleted_rows = insert_or_delete_rows(
                conn, "delete", table_name, commit=False
            )
            del_time_taken = get_time_elapsed(del_start_at)
            insert_start_at = time.perf_counter()
            inserted_rows = insert_or_delete_rows(
                conn, "insert", table_name, new_data, commit=False
            )
            insert_time_taken = get_time_elapsed(insert_start_at)
        except Exception:
            conn.rollback()  # type: ignore[attr-defined]
            cntlr.addToLog(
                "Refresh transaction rolled back due to error",
                **log_template("error", engine.url.database),
            )
            raise

        conn.commit()  # type: ignore[attr-defined]
        refs_del = [
            {
                "time": del_time_taken,
                "status": [
                    (
                        ts_now("utc"),
                        None,
                        table_name,
                        "delete",
                        deleted_rows,
                        del_time_taken,
                        True,
                    )
                ],
            }
        ]
        cntlr.addToLog(
            f"Deleted {deleted_rows:,} from {table_name} "
            f"in {del_time_taken:,} sec(s).",
            **log_template(constants.TSK_DELETE_TABLE, engine.url.database),
            refs=refs_del,
        )
        refs_ins = [
            {
                "time": insert_time_taken,
                "stats": [
                    (
                        ts_now("utc"),
                        999999,
                        table_name,
                        "insert",
                        inserted_rows,
                        insert_time_taken,
                        True,
                    )
                ],
            }
        ]
        cntlr.addToLog(
            f"Inserted {inserted_rows:,} into {table_name} in "
            f"{insert_time_taken:,} sec(s).",
            **log_template(constants.TSK_INSERT_TABLE, engine.url.database),
            refs=refs_ins,
        )


def prep_initialization_data(
    cntlr: Cntlr, update_data: bool = True
) -> dict[str, Any]:
    """Fetch data from local store or from online as necessary
    to populate initial tables
    """
    if update_data:
        # updates pickle files for these tables' data
        for func_ in updatable_tables_functions.values():
            func_(cntlr)

    allowed_tables = BASE.metadata.tables.keys()
    initialization_data = {}
    pickles = {
        x.split("-")[0]: os.path.join(getattr(cntlr, "db_cache_dir"), x)
        # {table_name: file_path, ...}
        for x in os.listdir(getattr(cntlr, "db_cache_dir"))
        if x.split("-")[0] in allowed_tables
    }
    for tablename, file_path in pickles.items():
        with open(file_path, "rb") as _pd:
            initialization_data[tablename] = pickle.load(_pd)
    # add locations from constants
    locs = BASE_M.Location
    initialization_data[getattr(locs, "__tablename__")] = [
        {
            locs.code.key: k,
            locs.latitude.key: v[2],
            locs.longitude.key: v[3],
            locs.country.key: v[0],
            locs.state_province.key: v[1],
            locs.location_fix.key: v[4],
            locs.alpha_2.key: v[5],
            locs.alpha_3.key: v[6],
            locs.numeric.key: v[7],
        }
        for k, v in constants.STATE_CODES.items()
    ]

    return initialization_data


def initialize_db(
    cntlr: Cntlr,
    engine: Engine,
    metadata: MetaData,
    initialization_data: dict[str, Any] | None = None,
    drop_first: bool = False,
) -> bool:
    """Creates model tables and inserts initialization data if available.
    If drop_first, drops all model tables first
    initialization_date is a dict {'table_name':[{row data}, ...]}
    """
    result = False

    if drop_first:
        cntlr.addToLog(
            ("Dropping all model tables..."),
            **log_template("info", engine.url.database),
        )
        metadata.drop_all(engine)

    model_tables = set(metadata.tables.keys())
    cntlr.addToLog(
        ("Initializing database..."),
        **log_template("info", engine.url.database),
    )

    metadata.create_all(engine)
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if len(model_tables - existing_tables) == 0:
        result = True
        cntlr.addToLog(
            ("Tables created, populating initial tables..."),
            **log_template("info", engine.url.database),
        )
    if result and initialization_data:
        # insert initialization data
        for tablename, table_data in initialization_data.items():
            refresh_table_data(engine, cntlr, tablename, table_data)
    return result


def verify_db(
    cntlr: Cntlr,
    engine: Engine,
    metadata: MetaData,
    initialize: bool = False,
    reinitialize: bool = False,
    is_test: bool = False,
) -> bool:
    """Check if all model table exists in the database, can create_tables, or
    force re-initialization of database, uses initialization_data if
    available to initialize tables.
    """
    result = True
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    model_tables = set(metadata.tables.keys())
    diff_in_tables = model_tables - existing_tables
    len_diff_in_tables = len(diff_in_tables)

    msg = (
        f'Database tables {", ".join(diff_in_tables)} are not available. '
        if len_diff_in_tables > 0
        else ""
    )
    if (len_diff_in_tables > 0 and initialize) or reinitialize:
        cntlr.addToLog(
            (f"{msg}Initializing database now..."),
            **log_template("info", engine.url.database),
        )
        initialization_data = prep_initialization_data(
            cntlr=cntlr, update_data=not is_test
        )

        # intercept data for all esef and sec filers if testing
        if is_test:
            for _t in constants.CACHED_CORE_TABLES:
                initialization_data.pop(_t, None)
        result = initialize_db(
            cntlr, engine, metadata, initialization_data, reinitialize
        )
    elif len_diff_in_tables > 0 and not initialize:
        cntlr.addToLog(
            (
                f"{msg}Database initialization NOT selected, "
                f"nothing to do here..."
            ),
            **log_template("info", engine.url.database),
        )
        result = False
    elif len_diff_in_tables == 0 and not reinitialize:
        result = True
    else:
        cntlr.addToLog(
            (
                f"Don't know what to do with {len_diff_in_tables}"
                f" missing tables and "
                f"initialize={initialize} and reinitialize={reinitialize} ..."
            ),
            **log_template("info", engine.url.database),
        )
        raise XIDBException(constants.ERR_DONT_KNOW)
    return result


def refresh_updatable_tables(
    cntlr: Cntlr, engine: Engine, table_name: str
) -> list[dict[str, Any]] | None:
    """Delete contents of tables that have cached data"""
    data = None
    try:
        data = updatable_tables_functions[table_name](cntlr)
    except Exception as ex:
        cntlr.addToLog(
            (f"Couldn't fetch {table_name} table data."),
            **log_template("error", engine.url.database, logging.ERROR),
        )
    if data is not None:
        refresh_table_data(engine, cntlr, table_name, data)
    return data


def make_db_connection(
    cntlr: Cntlr,
    metadata: MetaData,
    user: str,
    password: str,
    port: int,
    host: str,
    database: str,
    product: Literal["sqlite", "postgres", "mysql"],
    initialize: bool,
    timeout: int = 60,
) -> tuple[Cntlr, Engine, bool]:  # OK
    """Create and verify db connection"""
    start_time = time.perf_counter()
    if cntlr is None:
        cntlr = CntlrPy()
        cntlr.rss_dbname = database
    engine = create_connection_engine(
        user, password, host, port, database, product, None, timeout
    )
    # meta = rss_db_model.BASE.metadata
    db_exists = verify_db(cntlr, engine, metadata, initialize)
    time_taken = get_time_elapsed(start_time)
    msg = (
        f'{"Established" if db_exists else "Failed to establish"} '
        f"database connection in {time_taken} sec(s)"
    )
    cntlr.addToLog(
        msg,
        **log_template(constants.TSK_INITIALIZE_DB, engine.url.database),
        refs=[{"time": time_taken}],
    )
    return cntlr, engine, db_exists


def prep_monthly_feeds_to_process(
    cntlr: Cntlr,
    existing_feeds_dict: dict[str, datetime.datetime],
    database: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    include_latest: bool = True,
    loc: str | None = None,
) -> list[dict[str, Any]]:
    """Get and sort out SEC XBRL monthly rss feeds to update
    (https://www.sec.gov/Archives/edgar/monthly/)"""
    result: list[Any] = []
    start_time = time.perf_counter()
    _feeds: list[dict[str, Any]] = []
    _feeds = get_monthly_rss_feeds_links(cntlr, dbname=database, loc=loc)
    time_taken = get_time_elapsed(start_time)
    cntlr.addToLog(
        (f"Retrieved {len(_feeds)} feed links in {time_taken} sec(s)"),
        **log_template(
            constants.TSK_GET_FEEDS, getattr(cntlr, "rss_dbname", "")
        ),
        refs=[{"time": time_taken, "count": len(_feeds)}],
    )
    if not _feeds:
        return result
    feeds_date_filtered = date_filter_feeds(
        _feeds, from_date, to_date, include_latest
    )
    start_time = time.perf_counter()
    new_and_modified_feeds = get_new_and_modified_feeds(
        existing_feeds_dict, feeds_date_filtered
    )
    time_taken = get_time_elapsed(start_time)
    cntlr.addToLog(
        (
            f"found {len(new_and_modified_feeds)} "
            f"new and modified feeds in {time_taken} sec(s)"
        ),
        **log_template(constants.TSK_NEW_OR_MODIFIED_FEEDS, cntlr),
        refs=[{"time": time_taken, "count": len(new_and_modified_feeds)}],
    )
    return new_and_modified_feeds


def load_and_prep_feed(
    cntlr: Cntlr,
    engine: Engine,
    feed: dict[str, Any],
    reload_cache: bool = True,
) -> tuple[dict[str, Any], int, int, list[Any], ModelXbrl, bool, bool, bool]:
    """Loads feed and returns data necessary to process database update"""
    is_modified = feed["new_or_modified"] == "modified"
    is_new = feed["new_or_modified"] == "new"
    is_latest = feed["link"] == constants.LATEST_FEEDS_URL
    if feed["is_last_month"]:  # make sure we reload_cache for last feed
        reload_cache = True
    modelXbrl = load_rss_feed(feed["link"], cntlr, reload_cache=reload_cache)
    if not modelXbrl:
        raise XIDBException(constants.ERR_FEED_NOT_LOADED)
    feed_data, modelDoc = get_feed_info(
        modelXbrl, feed[str(SEC.SecFeed.last_modified_date.key)]
    )
    feed_id = feed_data["feed_id"]
    # assumes we will not have more than 100,000 filing a month ...
    initial_filing_id = int(str(feed_id) + "100001")
    if is_modified or is_latest:
        # get last filing_id for this feed in the db
        with Session(engine) as session:
            last_feed_id = (
                session.query(func.max(SEC.SecFiling.filing_id))
                .where(
                    SEC.SecFiling.feed_id == feed_id
                )  # pylint: disable=W0143
                .all()
            )
            last_feed_id_ = last_feed_id[0][0]
            initial_filing_id = (
                last_feed_id_ + 1
                if isinstance(last_feed_id_, int) and last_feed_id_ is not None
                else initial_filing_id
            )
    new_or_modified_filings = get_new_or_modified_filings(
        engine, feed_id, modelDoc, is_modified
    )
    return (
        feed_data,
        feed_id,
        initial_filing_id,
        new_or_modified_filings,
        modelXbrl,
        is_modified,
        is_new,
        is_latest,
    )


def extract_filings_data_from_rss_items(
    feed_id: int,
    initial_filing_id: int,
    new_or_modified_filings: list[ModelRssItem],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Extracts filing information from rss item created from loading
    an SEC XBRL rss feed"""
    filing_id = initial_filing_id
    filings_list = []
    files_list = []
    count_of_feed_files = 0
    for rss_item in new_or_modified_filings:
        filing_info_dict = get_filing_info(rss_item, feed_id, filing_id)
        files_info_list = get_files_info(rss_item, feed_id, filing_id)
        count_of_feed_files += len(files_info_list)
        filings_list.append(filing_info_dict)
        files_list.extend(files_info_list)
        filing_id += 1
    # add the difference then at the end mark duplicates if
    # any (duplicates occur due to changes made after month end)
    filings_list.sort(
        key=lambda x: cast(
            datetime.datetime, x[str(SEC.SecFiling.pub_date.key)]
        )
    )
    files_list.sort(key=lambda x: cast(int, x[str(SEC.SecFile.file_id.key)]))
    return filings_list, files_list


def insert_feed_into_db(
    engine: Engine,
    feed_data: dict[str, Any],
    filings_to_insert: list[dict[str, Any]],
    files_to_insert: list[dict[str, Any]],
    is_modified: bool,
) -> defaultdict[int, list[Any]]:
    """Insert prepared feed data into database"""
    stats: defaultdict[int, list[Any]] = defaultdict(list)
    feed_id = feed_data["feed_id"]
    feed_table = BASE.metadata.tables[str(SEC.SecFeed.__tablename__)]
    filing_table = BASE.metadata.tables[str(SEC.SecFiling.__tablename__)]
    file_table = BASE.metadata.tables[str(SEC.SecFile.__tablename__)]
    is_latest = feed_data["feed_link"] == constants.LATEST_FEEDS_URL
    with engine.connect() as conn1:
        if not is_latest:
            if is_modified:
                update_feed_stmt = (
                    update(feed_table)
                    .where(feed_table.c.feed_id == feed_id)
                    .values(**feed_data)
                )
                feed_update_start = time.perf_counter()
                cur1 = conn1.execute(update_feed_stmt)
                feed_insert_time = get_time_elapsed(feed_update_start)
                stats[feed_id].append(
                    (
                        ts_now("utc"),
                        feed_id,
                        feed_table.name,
                        "update",
                        cur1.rowcount,
                        feed_insert_time,
                        False,
                    )
                )
            else:
                feed_insert_start = time.perf_counter()
                rowcount_1 = insert_or_delete_rows(
                    conn1, "insert", feed_table.name, feed_data
                )
                feed_insert_time = get_time_elapsed(feed_insert_start)
                stats[feed_id].append(
                    (
                        ts_now("utc"),
                        feed_id,
                        feed_table.name,
                        "insert",
                        rowcount_1,
                        feed_insert_time,
                        False,
                    )
                )
        if len(filings_to_insert) > 0:
            filing_insert_start = time.perf_counter()
            rowcount_2 = insert_or_delete_rows(
                conn1, "insert", filing_table.name, filings_to_insert
            )
            filing_insert_time = get_time_elapsed(filing_insert_start)
            stats[feed_id].append(
                (
                    ts_now("utc"),
                    feed_id,
                    filing_table.name,
                    "insert",
                    rowcount_2,
                    filing_insert_time,
                    False,
                )
            )
        if len(files_to_insert) > 0:
            file_insert_start = time.perf_counter()
            rowcount_3 = insert_or_delete_rows(
                conn1, "insert", file_table.name, files_to_insert
            )
            file_insert_time = get_time_elapsed(file_insert_start)
            stats[feed_id].append(
                (
                    ts_now("utc"),
                    feed_id,
                    file_table.name,
                    "insert",
                    rowcount_3,
                    file_insert_time,
                    False,
                )
            )
        commit_start = time.perf_counter()
        conn1.commit()  # type: ignore[attr-defined]
        commit_time = get_time_elapsed(commit_start)
        stats[feed_id].append(
            (
                ts_now("utc"),
                feed_id,
                None,
                "commit-update-insert-feed-data",
                None,
                commit_time,
                True,
            )
        )
        for i, _t in enumerate(stats[feed_id]):
            stats[feed_id][i] = _t[:-1] + (True,)
    return stats


def insert_log(
    cntlr: Cntlr,
    engine: Engine,
    clear_buffer: bool = False,
    task_id: int | None = None,
) -> tuple[list[Any], list[Any]]:
    """Insert logs in log buffer from rss-db-log-handler"""
    processing_log: list[Any] = []
    action_log: list[Any] = []
    log_buffer = None
    logger: logging.Logger | None = cntlr.logger
    handler: logging.Handler | None = None
    assert isinstance(logger, logging.Logger)
    for _handler in logger.handlers:
        if _handler.name == RSS_DB_LOG_HANDLER_NAME:
            handler = _handler
            log_buffer = getattr(_handler, "logRecordBuffer")
            break
    assert isinstance(handler, IndexDBLogHandler)
    if log_buffer is not None and len(log_buffer) > 0:
        processing_log, action_log = handler.get_log_records()
        with engine.connect() as conn:
            if len(processing_log) > 0:
                for p_r in processing_log:
                    p_r["task_id"] = task_id
                insert_or_delete_rows(
                    conn,
                    "insert",
                    getattr(BASE_M.ProcessingLog, "__tablename__"),
                    processing_log,
                    True,
                )
            if len(action_log) > 0:
                for a_r in action_log:
                    a_r["task_id"] = task_id
                insert_or_delete_rows(
                    conn,
                    "insert",
                    getattr(BASE_M.ActionLog, "__tablename__"),
                    action_log,
                    True,
                )
        if clear_buffer:
            handler.clearLogBuffer()
    return processing_log, action_log


def update_filer_information(
    engine: Engine,
    cntlr: Cntlr,
    cik: str,
    is_existing: bool,
    url: str | None = None,
) -> tuple[bool, str | None]:
    """Get SEC filer information from SEC website"""
    filer_info = {}
    try:
        _filer_info = get_filer_information(cntlr, cik, 5, url)
        filer_info = _filer_info["filer"]
    except Exception as err:
        cntlr.addToLog(
            f"Could not get information for cik number: {cik}",
            **log_template("error", engine.url.database),
        )
        return False, f"cik:{cik}|" + str(err)
    try:
        with Session(engine) as session:
            if is_existing:
                filer: Any = (
                    session.query(SEC.SecFiler)
                    .filter(SEC.SecFiler.cik_number == cik)
                    .first()
                )
                filer.conformed_name = filer_info["conformed_name"]
                existing_former_names_tuples = {
                    (x.date_changed, x.name) for x in filer.former_names
                }
                new_former_names_dicts = filer_info["former_names"]
                unique_new_former_names_tuples = {
                    (x["date_changed"], x["name"])
                    for x in new_former_names_dicts
                    if x["date_changed"] and x["name"]
                }
                former_names_to_append = [
                    SEC.SecFormerNames(  # type: ignore[call-arg]
                        cik_number=cik, date_changed=x[0], name=x[1]
                    )
                    for x in (
                        unique_new_former_names_tuples
                        - existing_former_names_tuples
                    )
                ]
                filer.former_names.extend(former_names_to_append)
            else:
                former_names_dicts = filer_info.pop("former_names", [])
                if former_names_dicts:
                    unique_former_names_tuples = {
                        (x["date_changed"], x["name"])
                        for x in former_names_dicts
                        if x["date_changed"] and x["name"]
                    }
                    former_names_instances = [
                        SEC.SecFormerNames(  # type: ignore[call-arg]
                            cik_number=cik, date_changed=x[0], name=x[1]
                        )
                        for x in unique_former_names_tuples
                    ]
                    filer_info["former_names"] = former_names_instances
                filer = SEC.SecFiler(**filer_info)
                session.add(filer)
            session.commit()
        return True, None
    except Exception as err:
        return False, f"cik:{cik}|" + str(err)
