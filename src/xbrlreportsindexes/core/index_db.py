"""Class to interact with database
Connect and initialize database:\n
db = XbrlIndexDB.verify_initialize_database(initialize=True)

Update SEC filings index:\n
db.update_sec_default()

Update ESEF Filings index:\n
db.update_esef_default()

"""
from __future__ import annotations

import datetime
import gettext
import json
import logging
import os
import pathlib
import time
from io import BytesIO
from typing import Any
from typing import Literal

import pytz
from dateutil import parser
from distutils import dir_util
from lxml import etree
from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Date
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy import update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import aliased
from sqlalchemy.orm import Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from sqlalchemy.sql import false
from sqlalchemy.sql import label
from sqlalchemy.sql import operators
from xbrlreportsindexes.core import constants
from xbrlreportsindexes.core.arelle_utils import CntlrPy
from xbrlreportsindexes.core.arelle_utils import IndexDBLogHandler
from xbrlreportsindexes.core.constants import log_template
from xbrlreportsindexes.core.constants import XIDBException
from xbrlreportsindexes.core.data_utils import chunks
from xbrlreportsindexes.core.data_utils import extract_esef_entity_lei_info
from xbrlreportsindexes.core.data_utils import get_esef_all_filing_info
from xbrlreportsindexes.core.data_utils import get_esef_filings_index
from xbrlreportsindexes.core.data_utils import get_time_elapsed
from xbrlreportsindexes.core.data_utils import infer_esef_filing_language
from xbrlreportsindexes.core.data_utils import pickle_table_data
from xbrlreportsindexes.core.data_utils import truncate_string
from xbrlreportsindexes.core.data_utils import ts_now
from xbrlreportsindexes.core.db_utils import create_connection_engine
from xbrlreportsindexes.core.db_utils import (
    extract_filings_data_from_rss_items,
)
from xbrlreportsindexes.core.db_utils import insert_feed_into_db
from xbrlreportsindexes.core.db_utils import insert_log
from xbrlreportsindexes.core.db_utils import load_and_prep_feed
from xbrlreportsindexes.core.db_utils import prep_monthly_feeds_to_process
from xbrlreportsindexes.core.db_utils import refresh_updatable_tables
from xbrlreportsindexes.core.db_utils import update_filer_information
from xbrlreportsindexes.core.db_utils import verify_db
from xbrlreportsindexes.model import BASE
from xbrlreportsindexes.model import BASE_M
from xbrlreportsindexes.model import ESEF
from xbrlreportsindexes.model import SEC
from xbrlreportsindexes.model.types_mapping import random_function


try:
    from arelle.ModelXbrl import ModelXbrl, create
    from arelle.ModelRssObject import ModelRssObject
    from arelle.Cntlr import Cntlr
    from arelle.ModelRssItem import ModelRssItem
    from arelle import ModelObjectFactory as ObjFactory, ModelDocument
    from arelle import XmlUtil
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc


def initialize_cache_dir(
    user_app_dir: pathlib.Path, is_test: bool = False
) -> pathlib.Path:
    """Create a copy of source cache in arelle cache dir, or return
    source cache dir if testing.
    """
    dir_name: pathlib.Path | None = None
    this_dir: pathlib.Path = pathlib.Path(__file__).parent
    path_to_cache: pathlib.Path = this_dir.parent.joinpath("cache")
    index_db_cache_dir: pathlib.Path = user_app_dir.joinpath(
        constants.LOCAL_CACHE_DIR_NAME
    )
    if is_test:
        # return original cache from package
        dir_name = path_to_cache
    else:
        # use/create cache in arelle cache folder
        if index_db_cache_dir.is_dir():
            index_db_cache_dir.rmdir()
        index_db_cache_dir.mkdir()
        dir_util.copy_tree(
            path_to_cache.as_posix(), index_db_cache_dir.as_posix()
        )
        dir_name = index_db_cache_dir
    assert isinstance(dir_name, pathlib.Path)
    return dir_name


class XbrlIndexDB:
    """Filings index database instance"""

    cntlr: Cntlr | CntlrPy
    database: str | None
    metadata: MetaData
    product: Literal["sqlite", "postgres", "mysql"]
    engine: Engine
    current_task_tracker: BASE_M.TaskTracker | None
    tracker_session: Session | None
    db_cache_dir: pathlib.Path | None
    _db_mock_test_data_dir: pathlib.Path | None
    is_test: bool

    def __init__(
        self,
        # metadata: MetaData,
        database: str,
        product: Literal["sqlite", "postgres", "mysql"] = "sqlite",
        cntlr: Cntlr | None = None,
        user: str | None = None,
        password: str | None = None,
        host: str | None = None,
        port: int | None = None,
        connection_string: str | None = None,
        timeout: int | None = 60,
        reset_cache: bool = False,
        is_test: bool = False,
        verbose: bool = True,
        test_data_dir: pathlib.Path | None = None,
    ) -> None:
        self.is_test = is_test
        logging.getLogger("arelle").addHandler(
            IndexDBLogHandler(None, verbose=verbose, is_test=self.is_test)
        )
        self.cntlr = (
            cntlr if cntlr is not None else CntlrPy(logFileName="logToBuffer")
        )
        self.db_cache_dir = self.verify_cache_dir(
            reset=reset_cache, is_test=is_test
        )
        if self.is_test:
            assert isinstance(test_data_dir, pathlib.Path)
            assert test_data_dir.is_dir()
        self._db_mock_test_data_dir = test_data_dir
        self.cntlr.webCache.workOffline = is_test
        self.database = database
        self.metadata = BASE.metadata
        setattr(self.metadata, "cntlr", self.cntlr)
        self.product = product
        if product == "sqlite":
            if timeout is None or timeout < 600:
                timeout = 600
                self.cntlr.addToLog(
                    "Timeout is increased to 600 secs to accommodate "
                    "long file locks.",
                    **log_template("info", database),
                )
        self.engine = create_connection_engine(
            user,
            password,
            host,
            port,
            database,
            product,
            connection_string,
            timeout,
        )

        setattr(self.cntlr, "db_cache_dir", self.db_cache_dir.as_posix())
        setattr(self.engine, "db_cache_dir", self.db_cache_dir.as_posix())
        setattr(self.cntlr, "rss_dbname", database)
        self.db_exists = False
        self.current_task_tracker = None
        self.tracker_session = None
        self.verify_initialize_database()

    @classmethod
    def make_test_db(
        cls, test_data_dir: pathlib.Path, verbose: bool = True
    ) -> XbrlIndexDB:
        """Creates a test db suitable for running tests"""
        db = cls(
            database=":memory:",
            is_test=True,
            verbose=verbose,
            test_data_dir=test_data_dir,
        )
        db.verify_initialize_database(True)
        return db

    def verify_cache_dir(
        self, reset: bool = False, is_test: bool = False
    ) -> pathlib.Path:
        """Locate and verify cache dir"""
        if isinstance(self.cntlr, (Cntlr, CntlrPy)):
            user_app_dir: pathlib.Path = pathlib.Path(self.cntlr.userAppDir)
            if is_test:
                return initialize_cache_dir(user_app_dir, True)
            index_db_cache_dir: pathlib.Path = user_app_dir.joinpath(
                constants.LOCAL_CACHE_DIR_NAME
            )
            if not index_db_cache_dir.is_dir() or reset:
                index_db_cache_dir = initialize_cache_dir(user_app_dir)
            return index_db_cache_dir
        raise XIDBException(constants.ERR_NO_CNTLR, "Cntlr not found")

    def verify_initialize_database(
        self, initialize: bool = False, reinitialize: bool = False
    ) -> bool:
        """Checks schema, and initializes it if initialize is specified,
        drops and recreates everything i reinitialize is specified
        """
        start_time = time.perf_counter()
        db_exists = verify_db(
            self.cntlr,
            self.engine,
            self.metadata,
            initialize=initialize,
            reinitialize=reinitialize,
            is_test=self.is_test,
        )
        time_taken = get_time_elapsed(start_time)
        self.db_exists = db_exists
        msg = (
            "Verified/Initialized "
            if db_exists
            else "Failed to verify/initialize "
            + f"database in {time_taken} sec(s)"
        )
        self.cntlr.addToLog(
            msg,
            **log_template(
                constants.TSK_INITIALIZE_DB, self.engine.url.database
            ),
            refs=[{"time": time_taken}],
        )
        if db_exists:
            self.insert_log()
        return db_exists

    def insert_log(
        self, clear_buffer: bool = True
    ) -> tuple[list[Any], list[Any]]:
        """Insert log buffer into db"""
        task_id = None
        if self.current_task_tracker is not None:
            task_id = int(self.current_task_tracker.task_id)
        processing_log_rows, action_log_rows = insert_log(
            self.cntlr, self.engine, clear_buffer, task_id
        )
        return processing_log_rows, action_log_rows

    def get_new_and_modified_monthly_feeds(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        include_latest: bool = True,
        loc: str | None = None,
    ) -> list[dict[str, Any]]:
        """'Figure which feeds are new and which feed have
        modified date later than insert date"""
        new_and_modified_feeds = prep_monthly_feeds_to_process(
            cntlr=self.cntlr,
            existing_feeds_dict=self.get_existing_feeds(),
            database=self.database,
            from_date=from_date,
            to_date=to_date,
            include_latest=include_latest,
            loc=loc,
        )
        _x, _y = self.insert_log()
        return new_and_modified_feeds

    def get_existing_feeds(self) -> dict[str, datetime.datetime]:
        """Returns summary of existing feeds"""
        existing_feeds_dict = {}
        with Session(self.engine) as session:
            existing_feeds = session.query(
                SEC.SecFeed.feed_id, SEC.SecFeed.last_modified_date
            ).all()
            existing_feeds_dict = {
                x[0]: parser.parse(
                    x[1], tzinfos={"EST": "UTC-5:00", "EDT": "UTC-4:00"}
                )
                if isinstance(x[1], str)
                else x[1]
                for x in existing_feeds
            }
        return existing_feeds_dict

    def _load_and_prep_feed(
        self,
        feed_data_dict: dict[str, Any],
        reload_cache: bool = False,
        log_to_db: bool = True,
    ) -> tuple[
        dict[str, Any], int, int, list[Any], ModelXbrl, bool, bool, bool
    ]:
        """Loads rss feed and prepares data"""
        start_time = time.perf_counter()
        (
            feed_data,
            feed_id,
            initial_filing_id,
            new_or_modified_filings,
            modelXbrl,
            is_modified,
            is_new,
            is_latest,
        ) = load_and_prep_feed(
            self.cntlr, self.engine, feed_data_dict, reload_cache
        )
        time_taken = get_time_elapsed(start_time)
        self.cntlr.addToLog(
            f"Found {len(new_or_modified_filings)} "
            f"new or modified filings in {time_taken} sec(s).",
            **log_template(
                constants.TSK_NEW_OR_MODIFIED_FILINGS, feed_data["feed_link"]
            ),
            refs=[
                {
                    "time": time_taken,
                    "count": len(new_or_modified_filings),
                    "feed_id": feed_id,
                }
            ],
        )

        if log_to_db:
            _x, _y = self.insert_log()

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

    def _extract_filing_data_from_rss_item(
        self,
        feed_data: dict[str, Any],
        feed_id: int,
        initial_filing_id: int,
        new_or_modified_filings: list[ModelRssItem],
        log_to_db: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """extract filings and files data from rss items"""
        start_time = time.perf_counter()
        filings_list, files_list = extract_filings_data_from_rss_items(
            feed_id, initial_filing_id, new_or_modified_filings
        )
        time_taken = get_time_elapsed(start_time)
        self.cntlr.addToLog(
            f"Retrieved data of {len(filings_list)} "
            f"filings in {time_taken} sec(s).",
            **log_template(
                constants.TSK_GET_FEED_DATA, feed_data["feed_link"]
            ),
            refs=[
                {
                    "time": time_taken,
                    "count": len(filings_list),
                    "feed_id": feed_id,
                }
            ],
        )

        if log_to_db:
            _x, _y = self.insert_log()

        return filings_list, files_list

    def _insert_feed_into_db(
        self,
        feed_data: dict[str, Any],
        feed_id: int,
        is_latest: bool,
        filings_list: list[dict[str, Any]],
        files_list: list[dict[str, Any]],
        is_modified: bool,
        log_to_db: bool = True,
    ) -> list[Any] | None:
        """Insert feed data into db"""
        result: list[Any] | None = None
        # insert feed data into db
        db_insert_start = time.perf_counter()
        insert_update_stats = insert_feed_into_db(
            self.engine, feed_data, filings_list, files_list, is_modified
        )
        time_taken = get_time_elapsed(db_insert_start)
        feed_id_msg = "latest filings feed" if is_latest else f"feed {feed_id}"
        self.cntlr.addToLog(
            f"Finished insert of {feed_id_msg} in {time_taken} sec(s).",
            **log_template(
                constants.TSK_FEED_DB_INSERT, feed_data["feed_link"]
            ),
            refs=[{"time": time_taken, "stats": insert_update_stats[feed_id]}],
        )

        if log_to_db:
            _x, _y = self.insert_log()

        if insert_update_stats:
            result = insert_update_stats[feed_id]

        return result

    def insert_feed_data(
        self, feed_data_dict: dict[str, Any], reload_cache: bool = True
    ) -> tuple[bool, list[Any] | Exception | None]:
        """Wrapper for the whole process of load,
        extract and insert feed data"""
        modelXbrl = None
        if self.db_exists:
            try:
                (
                    feed_data,
                    feed_id,
                    initial_filing_id,
                    new_or_modified_filings,
                    modelXbrl,
                    is_modified,
                    _is_new,
                    is_latest,
                ) = self._load_and_prep_feed(feed_data_dict, reload_cache)
                (
                    filings_list,
                    files_list,
                ) = self._extract_filing_data_from_rss_item(
                    feed_data,
                    feed_id,
                    initial_filing_id,
                    new_or_modified_filings,
                )
                stats = self._insert_feed_into_db(
                    feed_data,
                    feed_id,
                    is_latest,
                    filings_list,
                    files_list,
                    is_modified,
                )
                # clean up
                modelXbrl.close()
                self.cntlr.modelManager.close(modelXbrl)
                del modelXbrl
                _x, _y = self.insert_log(True)
                return True, stats
            except Exception as err:
                if modelXbrl is not None:
                    modelXbrl.close()
                    self.cntlr.modelManager.close(modelXbrl)
                    del modelXbrl
                return False, err
        else:
            raise XIDBException(constants.ERR_NO_DB)

    def refresh_sp_companies_list(self) -> list[dict[str, Any]] | None:
        """Refreshes sp companies table data from wikipedia"""
        result = None
        if self.is_test:
            return result
        return refresh_updatable_tables(
            self.cntlr,
            self.engine,
            getattr(SEC.SpCompaniesCiks, "__tablename__"),
        )

    def refresh_sec_cik_ticker_mapping(self) -> list[dict[str, Any]] | None:
        """Refreshes cik ticker mapping from SEC website"""
        result = None
        if self.is_test:
            return result
        return refresh_updatable_tables(
            self.cntlr,
            self.engine,
            getattr(SEC.SecCikTickerMapping, "__tablename__"),
        )

    def make_tracker(self, task: str, **kwargs: Any) -> bool:
        """Checks if existing task is ongoing, if no blocking task,
        creates a task tracker and initializes it"""
        if self.current_task_tracker is not None:
            raise XIDBException(constants.ERR_EXISTING_TASK_IN_PROGRESS)
        ok_to_continue = True
        tracker_tbl = aliased(BASE_M.TaskTracker)
        existing = None
        with Session(self.engine) as session1:
            existing = (
                session1.query(tracker_tbl)
                .filter(
                    tracker_tbl.task_name == task,
                    tracker_tbl.is_closed == false(),
                )
                .first()
            )

        self.current_task_tracker = (
            BASE_M.TaskTracker(  # type: ignore[call-arg]
                process_id=os.getpid(),
                task_name=task,
                task_parameters=", ".join(
                    [f"{k}={v}" for k, v in kwargs.items()]
                ),
                total_items=0,
                completed_items=0,
                failed_items=0,
            )
        )

        self.tracker_session = Session(self.engine)
        if existing:
            msg = (
                f"Aborted: {task} is already running with process id "
                f"{existing.process_id} and task_id {existing.task_id}, "
                f"closing this task."
            )
            self.current_task_tracker.is_closed = True
            self.current_task_tracker.is_interrupted = True
            self.current_task_tracker.task_notes = msg
            ok_to_continue = False
            self.cntlr.addToLog(msg, **log_template("warning", self.database))
        self.tracker_session.add(self.current_task_tracker)
        self.cntlr.addToLog(
            f"Starting {task}", **log_template("warning", self.database)
        )
        self.tracker_session.commit()
        self.insert_log()
        return ok_to_continue

    def _advance_tracker_counts(
        self, count: int, is_successful: bool = True, note: str | None = None
    ) -> None:
        """Advances count of completed items for the current task, with note"""
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        self.current_task_tracker.completed_items += count
        self.current_task_tracker.successful_items += (
            count if is_successful else 0
        )
        self.current_task_tracker.failed_items += (
            count if not is_successful else 0
        )
        if isinstance(note, str):
            existing_error: str = (
                str(self.current_task_tracker.task_notes) + "|"
                if isinstance(self.current_task_tracker.task_notes, str)
                else ""
            )
            self.current_task_tracker.task_notes = (
                f"{existing_error}{truncate_string(note)}"
            )
        self.tracker_session.commit()  # type: ignore[union-attr]

    def _close_tracker(
        self,
        is_completed: bool = True,
        is_interrupted: bool = False,
        note: str | None = None,
    ) -> None:
        """Close current task and cleans up"""
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        self.current_task_tracker.ended_at = (
            ts_now() if self.product == "sqlite" else ts_now("utc")
        )
        self.current_task_tracker.time_taken = round(
            (
                self.current_task_tracker.ended_at
                - self.current_task_tracker.started_at
            ).total_seconds(),
            3,
        )
        self.current_task_tracker.is_closed = True
        self.current_task_tracker.is_interrupted = is_interrupted
        self.current_task_tracker.is_completed = is_completed
        if isinstance(note, str):
            existing_error = (
                str(self.current_task_tracker.task_notes) + "|"
                if self.current_task_tracker.task_notes
                else ""
            )
            self.current_task_tracker.task_notes = (
                f"{existing_error}{truncate_string(note)}"
            )
        # if self.current_task_tracker.task_notes == "|":
        #     self.current_task_tracker.task_notes = None
        last_updated = BASE_M.LastUpdate(
            task=self.current_task_tracker.task_name,
            last_updated=ts_now("utc"),
        )  # type: ignore[call-arg]
        assert isinstance(self.tracker_session, Session)
        self.tracker_session.add(last_updated)
        self.tracker_session.commit()
        self.tracker_session.close()
        self.current_task_tracker = None
        self.tracker_session = None

    def update_feeds(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        include_latest: bool = True,
        reload_cache: bool = True,
        loc: str | None = None,
    ) -> None:
        """wrapper around all tasks to update SEC feeds load,
        filter, extract and insert"""
        ok_to_continue = self.make_tracker(
            constants.DB_UPDATE_FEEDS,
            from_date=from_date,
            to_date=to_date,
            include_latest=include_latest,
            reload_cache=reload_cache,
        )

        if not ok_to_continue:
            raise XIDBException(constants.ERR_NO_TRACKER)
        try:
            assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
            assert isinstance(self.tracker_session, Session)
            # load and prep feed list
            # from https://www.sec.gov/Archives/edgar/monthly/
            new_and_modified_feeds = self.get_new_and_modified_monthly_feeds(
                from_date=from_date,
                to_date=to_date,
                include_latest=include_latest,
                loc=loc,
            )
            self.current_task_tracker.total_items = len(
                new_and_modified_feeds
            ) + (1 if include_latest else 0)
            self.tracker_session.commit()

            # process each feed
            start_time = time.perf_counter()

            for feed_data_dict in new_and_modified_feeds:
                stats = self.insert_feed_data(feed_data_dict, reload_cache)
                self._advance_tracker_counts(
                    1,
                    bool(stats[0]),
                    getattr(stats[1], "orig", str(stats[1]))
                    if not stats[0]
                    else None,
                )

            if include_latest and not self.is_test:
                latest_feed_dict = {
                    "link": constants.LATEST_FEEDS_URL,
                    "new_or_modified": "latest",
                    "last_modified_date": None,
                    "is_last_month": False,
                }
                stats = self.insert_feed_data(latest_feed_dict, True)
                self._advance_tracker_counts(
                    1,
                    bool(stats[0]),
                    getattr(stats[1], "orig", str(stats[1]))
                    if not stats[0]
                    else None,
                )

            time_taken = get_time_elapsed(start_time)
            with_latest = (
                "and latest filings "
                if include_latest and not self.is_test
                else ""
            )
            self.cntlr.addToLog(
                f"Processed {len(new_and_modified_feeds)} "
                f"feeds {with_latest} in {time_taken} sec(s).",
                **log_template(constants.TSK_PROCESS_FEEDS, self.database),
                refs=[{"time": time_taken}],
            )
            _x, _y = self.insert_log(True)
        except Exception as err:
            note = truncate_string(str(err))
            self._close_tracker(
                is_completed=False, is_interrupted=True, note=note
            )
            self.cntlr.addToLog(
                f"Failed to insert feeds, error: {str(err)}",
                **log_template("error", self.database),
            )
            _x, _y = self.insert_log(True)
        self._close_tracker()

    def do_exit_cleanup(
        self, is_completed: bool, is_interrupted: bool, note: str | None = None
    ) -> None:
        """Cleans up before exit, closes current task in order not to
        block future tasks, and inserts logs"""
        self.insert_log()
        if (
            isinstance(self.current_task_tracker, BASE_M.TaskTracker)
            and isinstance(self.tracker_session, Session)
            and self.tracker_session.is_active
        ):
            self._close_tracker(
                is_completed=is_completed,
                is_interrupted=is_interrupted,
                note=note,
            )
        self.engine.dispose()
        self.cntlr.modelManager.close()
        self.cntlr.close()

    def close(self) -> None:
        """Close and clean up"""
        self.do_exit_cleanup(True, False)

    def get_changed_and_new_ciks(self) -> tuple[set[Any], set[Any]]:
        """Checks for new and modified filers information"""
        new_ciks = changed_ciks = set()
        with Session(self.engine) as session:
            cte_x = (
                session.query(
                    SEC.SecFiling.cik_number,
                    func.max(SEC.SecFiling.filing_id).label("filing_id"),
                )
                .group_by(SEC.SecFiling.cik_number)
                .cte(name="x")
            )
            cte_d = (
                session.query(
                    func.max(SEC.SecFormerNames.date_changed).label(
                        "date_changed"
                    ),
                    SEC.SecFormerNames.cik_number,
                )
                .join(
                    cte_x,
                    cte_x.c.cik_number == SEC.SecFormerNames.cik_number,
                    isouter=True,
                )
                .group_by(SEC.SecFormerNames.cik_number)
                .cte(name="d")
            )
            cte_y = (
                session.query(
                    SEC.SecFiling.cik_number,
                    SEC.SecFiling.company_name,
                    SEC.SecFiling.filing_id,
                    SEC.SecFiling.pub_date,
                )
                .join(cte_x, cte_x.c.filing_id == SEC.SecFiling.filing_id)
                .cte(name="y")
            )
            cte_n = (
                session.query(
                    cte_y.c.cik_number,
                    cte_y.c.company_name,
                    cte_y.c.filing_id,
                    SEC.SecFiler.conformed_name,
                    label(
                        "test",
                        and_(
                            func.lower(SEC.SecFiler.conformed_name)
                            != func.lower(cte_y.c.company_name),
                            cast(cte_y.c.pub_date, Date())
                            > cte_d.c.date_changed,
                        ),
                    ),
                )
                .select_from(cte_y)
                .join(
                    SEC.SecFiler,
                    SEC.SecFiler.cik_number == cte_y.c.cik_number,
                )
                .join(cte_d, cte_y.c.cik_number == cte_d.c.cik_number)
                .cte(name="n")
            )
            ciks_changed_names_qry = (
                session.query(cte_n)
                .filter(operators.is_(cte_n.c.test, True))
                .order_by(cte_n.c.cik_number)
            )
            changed_ciks = {x[0] for x in ciks_changed_names_qry}

            ciks_new_qry = session.query(
                SEC.SecFiling.cik_number.distinct()
            ).filter(
                ~exists().where(
                    SEC.SecFiler.cik_number == SEC.SecFiling.cik_number
                )
            )
            new_ciks = {x[0] for x in ciks_new_qry}
        return new_ciks, changed_ciks

    def _filer_action_loop(
        self,
        ciks: set[str],
        is_existing: bool,
        retries: int = 3,
        n_retry: int = 0,
    ) -> int:
        """Get and insert filers' information, when an update fails,
        retry retries times."""
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        assert isinstance(self.tracker_session, Session)
        failed_ciks = set()
        retry_text = f" (retry {n_retry})" if n_retry > 0 else ""
        action = (
            f"Updating{retry_text}"
            if is_existing
            else f"Inserting{retry_text}"
        )
        task_code = (
            constants.DB_UPDATE_EXISTING_FILERS
            if is_existing
            else constants.DB_INSERT_NEW_FILERS
        )
        start_time = time.perf_counter()
        print(constants.MSG_WHILE_SAVED, end="\n")
        for cik in ciks:
            time.sleep(0.2)  # SEC 10 requests per second limit
            url: str | None = None
            if self.is_test:
                assert isinstance(self._db_mock_test_data_dir, pathlib.Path)
                url = self._db_mock_test_data_dir.joinpath(
                    "sec", "ciks", cik
                ).as_uri()
            stats = update_filer_information(
                self.engine, self.cntlr, cik, is_existing, url
            )
            if not stats[0]:
                failed_ciks.add(cik)
            self._advance_tracker_counts(
                1,
                bool(stats[0]),
                getattr(stats[1], "orig", str(stats[1]))
                if not stats[0]
                else None,
            )
            print(
                f'{action}{"" if is_existing else " new"} cik:',
                self.current_task_tracker.completed_items,
                "/",
                self.current_task_tracker.total_items,
                "->",
                cik,
                end="\r",
            )
        time_taken = get_time_elapsed(start_time)

        successful = self.current_task_tracker.successful_items
        completed = self.current_task_tracker.completed_items
        failed = self.current_task_tracker.failed_items
        self.cntlr.addToLog(
            f"Finished {action.lower()} {successful:,} of "
            f"{completed:,} filers in {time_taken} sec(s) "
            f"with {failed:,} failed.",
            **log_template(task_code, self.database),
        )

        if len(failed_ciks) > 0 and retries > 0:
            self.current_task_tracker.total_items += len(failed_ciks)
            self.tracker_session.commit()
            self._filer_action_loop(
                failed_ciks, is_existing, retries - 1, n_retry + 1
            )

        return failed

    def _filers_insert_update(
        self,
        new_ciks_to_insert: set[str],
        existing_ciks_to_update: set[str],
        retries: int = 3,
        _n: int | None = None,
    ) -> None:
        """Insert filers updated data into db"""
        # for testing
        if isinstance(_n, int):
            if len(new_ciks_to_insert) > _n:
                new_ciks_to_insert = set(list(new_ciks_to_insert)[:_n])
            if len(existing_ciks_to_update) > _n:
                existing_ciks_to_update = set(
                    list(existing_ciks_to_update)[:_n]
                )
        if new_ciks_to_insert:
            ok_to_continue = self.make_tracker(constants.DB_INSERT_NEW_FILERS)
            assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
            assert isinstance(self.tracker_session, Session)
            self.current_task_tracker.total_items = len(new_ciks_to_insert)
            self.tracker_session.commit()

            if not ok_to_continue:
                raise XIDBException(constants.ERR_NO_TRACKER)

            self.cntlr.addToLog(
                f"Inserting {len(new_ciks_to_insert):,} new filers",
                **log_template(constants.DB_INSERT_NEW_FILERS, self.database),
            )
            # start action loop
            self._filer_action_loop(new_ciks_to_insert, False, retries)
            self._close_tracker(True, False)
            self.insert_log()

        if existing_ciks_to_update:
            ok_to_continue = self.make_tracker(
                constants.DB_UPDATE_EXISTING_FILERS
            )
            assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
            assert isinstance(self.tracker_session, Session)
            self.current_task_tracker.total_items = len(
                existing_ciks_to_update
            )
            self.tracker_session.commit()

            if not ok_to_continue:
                raise XIDBException(constants.ERR_NO_TRACKER)

            self.cntlr.addToLog(
                f"Updating {len(existing_ciks_to_update):,} "
                f"filers with changes detected",
                **log_template(
                    constants.DB_UPDATE_EXISTING_FILERS, self.database
                ),
            )
            # start update action loop
            self._filer_action_loop(existing_ciks_to_update, True, retries)
            self._close_tracker()
            self.insert_log()

        if not self.is_test:
            # refresh cached data
            if len(new_ciks_to_insert) > 0 or len(existing_ciks_to_update) > 0:
                self.cntlr.addToLog(
                    "Refreshing filers' cached data.",
                    **log_template("info", self.database),
                )
                start_time = time.perf_counter()
                cache_file_filer = pickle_table_data(self.engine, SEC.SecFiler)
                cache_file_names = pickle_table_data(
                    self.engine, SEC.SecFormerNames
                )
                time_taken = get_time_elapsed(start_time)
                self.cntlr.addToLog(
                    f"Finished refreshing filers' cached data "
                    f"({cache_file_filer}, {cache_file_names}) "
                    f"in {time_taken} sec(s).",
                    **log_template("info", self.database),
                )
                self.insert_log()

    def filers_quick_update(
        self, only_new: bool = False, retries: int = 3, _n: int | None = None
    ) -> None:
        """Quick check to discover if filers' information needs to
        be updated"""
        new_ciks, changed_ciks = self.get_changed_and_new_ciks()
        msg = f"found {len(new_ciks):,} new ciks"
        if only_new:
            msg += "."
            changed_ciks = set()
        else:
            msg += f", and {len(changed_ciks):,} modified ciks."
        self.cntlr.addToLog(msg, **log_template("info", self.database))
        try:
            self._filers_insert_update(new_ciks, changed_ciks, retries, _n)
        except (Exception, SystemExit) as err:
            note = truncate_string(str(err))
            self.do_exit_cleanup(False, True, note)

    def filers_update_by_date(
        self, before_date: str, retries: int = 3, _n: int | None = None
    ) -> None:
        """Updates filers created in this db before the specified date"""
        try:
            parser.parse(before_date)
        except Exception as err:
            raise XIDBException(constants.ERR_BAD_DATE) from err

        ciks_to_process = []
        with Session(self.engine) as session:
            ciks_qry = session.query(SEC.SecFiler.cik_number).filter(
                parser.parse(before_date)
                > cast(SEC.SecFiler.created_updated_at, Date())
            )
            ciks_to_process = [x[0] for x in ciks_qry]

        if len(ciks_to_process) > 0:
            try:
                self._filers_insert_update(
                    set(), set(ciks_to_process), retries, _n
                )
            except (Exception, SystemExit) as err:
                note = truncate_string(str(err))
                self.do_exit_cleanup(False, True, note)
        else:
            self.cntlr.addToLog(
                f"No ciks detected created before {before_date}",
                **log_template("info", self.database),
            )

    def filers_update_ciks(
        self,
        ciks_to_process: list[str],
        retries: int = 3,
        _n: int | None = None,
    ) -> None:
        """Updates insert filers with the specified ciks, ciks_to_process is
        a list of ciks ['0000000001', '0000000002', ...]"""

        if len(ciks_to_process) > 0:
            existing_ciks = set()
            new_ciks = set()
            with Session(self.engine) as session:
                existing = session.query(SEC.SecFiler.cik_number).filter(
                    SEC.SecFiler.cik_number.in_(ciks_to_process)
                )
                existing_ciks = {x[0] for x in existing}
                new_ciks = set(ciks_to_process) - existing_ciks
            try:
                self._filers_insert_update(
                    new_ciks, existing_ciks, retries, _n
                )
            except (Exception, SystemExit) as err:
                note = truncate_string(str(err))
                self.do_exit_cleanup(False, True, note)
        else:
            self.cntlr.addToLog(
                "No ciks were given to process.",
                **log_template("info", self.database),
            )

    def detect_and_tag_duplicates(self) -> None:
        """Find filing duplicates and tag them.
        Filings may be resubmitted due to corrupt or unusable files,
        this has no impact if resubmission was made during the same
        month as the original submission, but when done in a following
        month this will result in the filing appearing in the resubmission
        month, see https://www.sec.gov/os/accessing-edgar-data
        #:~:text=Post%2Dacceptance%20corrections%20and%20deletions)
        """
        ok_to_continue = self.make_tracker(constants.DB_PROCESS_DUPLICATES)
        if not ok_to_continue:
            return
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        assert isinstance(self.tracker_session, Session)

        self.cntlr.addToLog(
            "Checking for duplicates (see SEC website here "
            "https://www.sec.gov/os/accessing-edgar-data"
            "#:~:text=Post%2Dacceptance%20corrections%20and%20deletions)",
            **log_template(constants.DB_PROCESS_DUPLICATES, self.database),
        )
        start_time = time.perf_counter()
        existing_dups = 0
        with Session(self.engine) as session1:
            dups_check = session1.query(SEC.ViewDuplicateFiling).all()
            existing_dups = len(dups_check)
        # return
        if existing_dups > 0:
            self.cntlr.addToLog(
                f"Tagging {existing_dups:,} duplicates found.",
                **log_template(constants.DB_PROCESS_DUPLICATES, self.database),
            )
            self.current_task_tracker.total_items = existing_dups
            self.tracker_session.commit()
            duplicates: Any = []
            with Session(self.engine) as session2:
                duplicates = session2.query(SEC.SecFiling).join(
                    SEC.ViewDuplicateFiling,
                    SEC.SecFiling.filing_id
                    == (
                        SEC.ViewDuplicateFiling.filing_id  # type: ignore[attr-defined]
                    ),
                )
                try:
                    update_filings_stmt = (
                        update(SEC.SecFiling)
                        .where(
                            SEC.SecFiling.filing_id.in_(
                                [x.filing_id for x in duplicates]
                            )
                        )
                        .values(duplicate=1)
                    )
                    update_files_stmt = (
                        update(SEC.SecFile)
                        .where(
                            SEC.SecFile.filing_id.in_(
                                [x.filing_id for x in duplicates]
                            )
                        )
                        .values(duplicate=1)
                    )
                    filings_cur = session2.execute(update_filings_stmt)
                    filings_rows = (
                        filings_cur.rowcount  # type: ignore[attr-defined]
                    )
                    session2.execute(update_files_stmt)
                    session2.commit()
                    self._advance_tracker_counts(filings_rows, True)
                except Exception as err:
                    note = truncate_string(str(err))
                    self._advance_tracker_counts(0, False, note)
        time_taken = get_time_elapsed(start_time)
        self.cntlr.addToLog(
            f"Tagged {self.current_task_tracker.completed_items:,} "
            f"duplicate filing(s) from {existing_dups:,} duplicates "
            f"found in {time_taken} sec(s).",
            **log_template(constants.DB_PROCESS_DUPLICATES, self.database),
        )
        self._close_tracker()
        self.insert_log()

    def update_sec_default(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        include_latest: bool = True,
        reload_cache: bool = True,
        only_new: bool = False,
        retries: int = 3,
        _n: int | None = None,
        loc: str | None = None,
    ) -> None:
        """Perform all SEC feeds update tasks using default settings"""
        self.update_feeds(
            from_date=from_date,
            to_date=to_date,
            include_latest=include_latest,
            reload_cache=reload_cache,
            loc=loc,
        )
        self.detect_and_tag_duplicates()
        self.filers_quick_update(only_new=only_new, retries=retries, _n=_n)

    def get_esef_existing_filings_entities(self) -> tuple[set[Any], set[Any]]:
        """Get existing ESEF entities (LEI)"""
        start_time = time.perf_counter()
        with Session(self.engine) as session:
            existing_esef_filings = {
                x[0]
                for x in session.query(ESEF.EsefFiling.filing_key.distinct())
            }
            existing_esef_entities = {
                x[0]
                for x in session.query(ESEF.EsefEntity.entity_lei.distinct())
            }
        time_taken = get_time_elapsed(start_time)
        self.cntlr.addToLog(
            f"Retrieved existing ESEF filings and "
            f"entities in {time_taken} sec(s).",
            **log_template(constants.DB_GET_EXISTING_ESEF_INFO, self.database),
        )
        return existing_esef_filings, existing_esef_entities

    def get_esef_new_filings_new_entities(
        self, esef_index: dict[str, Any]
    ) -> tuple[set[Any], set[Any]]:
        """Compares existing filings to the downloaded index and
            returns new filing
        to insert."""
        # get existing filings and entities
        (
            existing_esef_filings,
            existing_esef_entities,
        ) = self.get_esef_existing_filings_entities()
        index_entities = set(esef_index.keys())
        index_filings = {
            filing_key
            for entity, filings in esef_index.items()
            for filing_key in filings["filings"]
        }
        new_esef_filings = index_filings - existing_esef_filings
        new_esef_entities = index_entities - existing_esef_entities
        return new_esef_entities, new_esef_filings

    def _insert_esef_filing(
        self, filing_addr: str, filing: dict[str, Any]
    ) -> None:
        """Helper to insert esef filing data"""
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        esef_filing, esef_error, esef_filing_langs = get_esef_all_filing_info(
            filing_addr=filing_addr, filing_dict=filing
        )
        errors = (
            [ESEF.EsefFilingError(**x) for x in esef_error]
            if esef_error
            else []
        )
        langs = (
            [ESEF.EsefFilingLang(**x) for x in esef_filing_langs]
            if esef_filing_langs
            else []
        )
        filing_inst = ESEF.EsefFiling(
            **esef_filing, errors=errors, langs=langs
        )
        # try to detect language from json instance
        if filing_inst.xbrl_json_instance_link is not None:
            json_xbrl_uri = filing_inst.xbrl_json_instance_link
            if self.is_test:
                assert isinstance(self._db_mock_test_data_dir, pathlib.Path)
                json_xbrl_uri = self._db_mock_test_data_dir.joinpath(
                    "esef", "json_xbrl", filing_inst.xbrl_json_instance
                ).as_uri()
            inferred_langs = infer_esef_filing_language(
                self.cntlr, json_xbrl_uri
            )
            if len(inferred_langs) > 0:
                inferred_langs_instances = [
                    ESEF.EsefInferredFilingLanguage(**x)
                    for x in inferred_langs
                ]
                filing_inst.inferred_langs = inferred_langs_instances
                time.sleep(0.2)  # give the api a break
        with Session(self.engine) as session:
            if esef_filing:
                session.add(filing_inst)
            session.commit()
        self._advance_tracker_counts(1, True)
        print(
            f"Inserted {self.current_task_tracker.completed_items} / "
            f"{self.current_task_tracker.total_items}",
            end="\r",
        )

    def insert_esef_new_filings(
        self, esef_index: dict[str, Any], new_esef_filings: set[str]
    ) -> int:
        """Insert new ESEF filings into db (if filing is in
        new_esef_filings, gets inserted.)"""
        failed = 0
        ok_to_continue = self.make_tracker(constants.DB_UPDATE_ESEF_FILINGS)
        if not ok_to_continue:
            self.insert_log()
            return failed
        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        assert isinstance(self.tracker_session, Session)
        self.current_task_tracker.total_items = len(new_esef_filings)
        self.tracker_session.commit()
        started_time = time.perf_counter()
        for _entity, entity_filings in esef_index.items():
            try:
                for filing_addr, filing in entity_filings["filings"].items():
                    if filing_addr in new_esef_filings:
                        self._insert_esef_filing(filing_addr, filing)
            except Exception as err:
                note = truncate_string(str(err))
                self._advance_tracker_counts(1, False, note)
                print(
                    f"Failed to insert "
                    f"{self.current_task_tracker.failed_items} / "
                    f"{self.current_task_tracker.total_items}",
                    end="\r",
                )
        time_taken = get_time_elapsed(started_time)
        total = self.current_task_tracker.total_items
        completed = self.current_task_tracker.completed_items
        failed = self.current_task_tracker.failed_items
        self.cntlr.addToLog(
            f"Finished inserting {completed:,} filings of {total:,}, "
            f"and failed {failed:,} in {time_taken} sec(s).",
            **log_template(constants.DB_UPDATE_ESEF_FILINGS, self.database),
        )
        self._close_tracker(True, False)
        self.insert_log()
        return failed

    def insert_esef_new_entities(self, new_esef_entities: set[str]) -> int:
        """Insert new ESEF entities discovered."""
        failed = 0
        ok_to_continue = self.make_tracker(constants.DB_UPDATE_ESEF_ENTITIES)
        if not ok_to_continue:
            self.insert_log()
            return failed

        assert isinstance(self.current_task_tracker, BASE_M.TaskTracker)
        assert isinstance(self.tracker_session, Session)
        self.current_task_tracker.total_items = len(new_esef_entities)
        self.tracker_session.commit()
        started_time = time.perf_counter()

        # chunks of 100 not to overwhelm the api
        chunked = chunks(list(new_esef_entities), 100)
        lei_url = (
            "https://api.gleif.org/api/v1/lei-records?"
            "page[size]=100&filter[lei]="
        )

        for chunk in chunked:
            url: str = lei_url + ",".join(chunk)
            if self.is_test:
                assert isinstance(self._db_mock_test_data_dir, pathlib.Path)
                url = self._db_mock_test_data_dir.joinpath(
                    "esef", "lei"
                ).as_uri()
            lei_data_resp = self.cntlr.webCache.opener.open(url, timeout=5)
            lei_data = json.loads(lei_data_resp.read().decode())
            lei_data = lei_data["data"]
            if self.is_test:
                lei_data = [x for x in lei_data if x["id"] in chunk]
            for entity_lei_data in lei_data:
                try:
                    (
                        esef_entity,
                        esef_other_names,
                    ) = extract_esef_entity_lei_info(
                        self.cntlr,
                        entity_lei_data,
                        self.is_test,
                        self._db_mock_test_data_dir,
                    )
                    other_names = [
                        ESEF.EsefEntityOtherName(**other_name)
                        for other_name in esef_other_names
                    ]
                    entity = ESEF.EsefEntity(
                        **esef_entity, other_names=other_names
                    )
                    with Session(self.engine) as session:
                        session.add(entity)
                        # session.bulk_save_objects(other_names)
                        session.commit()
                    self._advance_tracker_counts(1, True)
                    print(
                        f"inserted "
                        f"{self.current_task_tracker.completed_items} / "
                        f"{self.current_task_tracker.total_items}",
                        end="\r",
                    )
                    time.sleep(0.2)
                except Exception as err:
                    note = truncate_string(str(err))
                    self._advance_tracker_counts(1, False, note)

        time_taken = get_time_elapsed(started_time)
        total = self.current_task_tracker.total_items
        completed = self.current_task_tracker.completed_items
        failed = self.current_task_tracker.failed_items
        self.cntlr.addToLog(
            f"Finished inserting {completed:,} Entities of "
            f"{total:,}, and failed {failed:,} in {time_taken} sec(s).",
            **log_template(constants.DB_UPDATE_ESEF_ENTITIES, self.database),
        )
        self._close_tracker(True, False)
        self.insert_log()
        return failed

    def esef_detect_amended_and_multi_langs(
        self,
    ) -> tuple[list[Any], list[Any]]:
        """Detect amended filings and filings that are issued in
        multiple languages"""
        amended_filings = multi_langs = []
        langs = aliased(ESEF.EsefInferredFilingLanguage)
        esef_filings = aliased(ESEF.EsefFiling)
        with Session(self.engine) as session:
            # if a filing has multiple langs, rank langs by fact
            # count for each lang
            sub_query_langs = (
                session.query(
                    langs.filing_id,
                    langs.lang,
                    func.rank()
                    .over(
                        order_by=langs.facts_in_lang.desc(),
                        partition_by=langs.filing_id,
                    )
                    .label("ranked"),
                )
            ).subquery(name="l")
            # get the lang with the highest fact count per
            # filing (one filing one lang)
            langs_ranked = (
                session.query(sub_query_langs)
                .filter(sub_query_langs.c.ranked == 1)
                .cte(name="langs_ranked")
            )

            # loadable files that are not marked as amended
            filings = (
                session.query(
                    esef_filings.filing_root,
                    esef_filings.report_package,
                    langs_ranked.c.lang,
                    esef_filings.date_added,
                    esef_filings.filing_id,
                    esef_filings.filing_number,
                )
                .select_from(esef_filings)
                .join(
                    langs_ranked,
                    esef_filings.filing_id == langs_ranked.c.filing_id,
                    isouter=True,
                )
                .where(
                    esef_filings.is_loadable,
                    not_(esef_filings.is_amended_hint),
                )
            ).cte(name="filings")

            grouped = (
                session.query(
                    filings.c.filing_root,
                    filings.c.report_package,
                    filings.c.lang,
                    func.count().label("xcount"),
                ).group_by(
                    filings.c.filing_root,
                    filings.c.report_package,
                    filings.c.lang,
                )
            ).cte(name="grouped")

            detect_last = (
                session.query(
                    grouped,
                    filings,
                    not_(
                        func.max(filings.c.filing_number).over(
                            partition_by=[
                                filings.c.filing_root,
                                filings.c.lang,
                            ]
                        )
                        == filings.c.filing_number
                    ).label("is_not_max"),
                )
                .select_from(grouped)
                .join(
                    filings,
                    (grouped.c.filing_root == filings.c.filing_root)
                    & (grouped.c.report_package == filings.c.report_package)
                    & (grouped.c.lang == filings.c.lang),
                )
                .where(grouped.c.xcount > 1)
            ).cte(name="detect_last")

            amended_filings_qry = session.query(detect_last.c.filing_id).where(
                detect_last.c.is_not_max
            )
            amended_filings = [x[0] for x in amended_filings_qry]
            # detect filings with multiple langs
            langs_per_root = (
                session.query(
                    esef_filings.filing_root, func.count().label("lang_count")
                )
                .select_from(esef_filings)
                .join(langs, langs.filing_id == esef_filings.filing_id)
                .group_by(esef_filings.filing_root)
                .where(not_(esef_filings.other_langs_hint))
            ).cte(name="langs_per_root")
            langs_qry = (
                session.query(esef_filings.filing_id)
                .select_from(esef_filings)
                .join(
                    langs_per_root,
                    langs_per_root.c.filing_root == esef_filings.filing_root,
                )
                .where(langs_per_root.c.lang_count > 1)
            )
            multi_langs = [x[0] for x in langs_qry]
        return amended_filings, multi_langs

    def tag_amended_and_multi_langs(self) -> None:
        """Tags amended and filings issued in multiple languages"""
        start_time = time.perf_counter()
        amended, multi_lang = self.esef_detect_amended_and_multi_langs()
        with Session(self.engine) as session:
            if len(amended) > 0:
                amended_qry = session.query(ESEF.EsefFiling).filter(
                    ESEF.EsefFiling.filing_id.in_(amended)
                )
                for filing in amended_qry:
                    filing.is_amended_hint = True
                session.commit()
            if len(multi_lang) > 0:
                multi_lang_qry = session.query(ESEF.EsefFiling).filter(
                    ESEF.EsefFiling.filing_id.in_(multi_lang)
                )
                for filing_ in multi_lang_qry:
                    filing_.other_langs_hint = True
                session.commit()
        time_taken = get_time_elapsed(start_time)
        self.cntlr.addToLog(
            f"Detected {len(amended):,} amended filings and "
            f"{len(multi_lang):,} filings issued in multiple "
            f"languages in {time_taken:,} sec(s).",
            **log_template(constants.DB_UPDATE_ESEF_FILINGS, self.database),
        )
        self.insert_log()

    def update_esef_default(
        self, retries: int = 3, n_retry: int = 0, loc: str | None = None
    ) -> None:
        """Wrapper around all tasks to update ESEF filings,
        uses default settings."""
        # get index
        retry_text = f" (retry {n_retry})" if n_retry > 0 else ""

        base_url = esef_index = None
        started_time = time.perf_counter()
        try:
            base_url, esef_index = get_esef_filings_index(
                self.cntlr, reload_cache=n_retry == 0, loc=loc
            )
        except Exception:
            pass
        time_taken = get_time_elapsed(started_time)
        self.cntlr.addToLog(
            f'{"Retrieved " if esef_index else "Failed to retrieve "}'
            f"ESEF{retry_text} index in {time_taken} sec(s).",
            **log_template(constants.TSK_ESEF_INDEX, self.database),
        )
        if not esef_index:
            self.insert_log()
            return

        # detect new filings and entities to insert
        (
            new_esef_entities,
            new_esef_filings,
        ) = self.get_esef_new_filings_new_entities(esef_index)
        self.cntlr.addToLog(
            f"Found {len(new_esef_filings):,} new ESEF filings and "
            f"{len(new_esef_entities):,} new ESEF entities {retry_text}.",
            **log_template("info", self.database),
        )
        self.insert_log()
        # insert new filings
        failed_filings = 0
        if new_esef_filings:
            failed_filings = self.insert_esef_new_filings(
                esef_index, new_esef_filings
            )

        # insert new entities
        failed_entities = 0
        if new_esef_entities:
            failed_entities = self.insert_esef_new_entities(new_esef_entities)
        self.insert_log()

        # tag amended and same filing in multiple langs
        self.tag_amended_and_multi_langs()

        if not self.is_test:
            start_time = time.perf_counter()
            for table_ in [
                ESEF.EsefEntity,
                ESEF.EsefEntityOtherName,
                ESEF.EsefFiling,
                ESEF.EsefFilingError,
                ESEF.EsefFilingLang,
                ESEF.EsefInferredFilingLanguage,
            ]:
                cache_file_name = pickle_table_data(self.engine, table_)

            time_taken = get_time_elapsed(start_time)
            self.cntlr.addToLog(
                f"Finished refreshing ESEF cached data ({cache_file_name})"
                f" in {time_taken} sec(s).",
                **log_template("info", self.database),
            )
            self.insert_log()

        if (failed_entities > 0 or failed_filings > 0) and n_retry < retries:
            self.update_esef_default(retries=retries, n_retry=n_retry + 1)
        return

    def _make_rss_feed(
        self,
        filings_list: Query[Any],
        filename: str,
        title: str | None = None,
        description: str | None = None,
    ) -> ModelXbrl:
        """Writes a list of filings as rss feed similar to SEC feeds that
        can be loaded by arelle."""
        time_format_tz = "%a, %d %b %Y %H:%M:%S %Z"
        link = filename
        date_time = datetime.datetime.now(tz=pytz.timezone("utc")).strftime(
            time_format_tz
        )
        # document header elements
        root = etree.Element("rss", version="2.0")
        channel = etree.SubElement(root, "channel")
        title_elements = {
            "title": title,
            "link": link,
            "description": description,
            "language": "en-us",
            "pubDate": date_time,
            "lastBuildDate": date_time,
        }
        for tag, value in title_elements.items():
            etree.SubElement(channel, tag).text = value
        etree.SubElement(
            channel,
            etree.QName("https://www.w3.org/2005/Atom", tag="link"),
            nsmap={"atom": "https://www.w3.org/2005/Atom"},
            href=link,
            rel="self",
            type="application/rss+xml",
        )
        for filing_object in filings_list:
            assert isinstance(self.database, str)
            filing_object.to_xml(channel, self.database)

        root_string = BytesIO(etree.tostring(root.getroottree()))
        model_xbrl: ModelXbrl = create(self.cntlr.modelManager)
        _parser, _parser_lookup_name, _parser_lookup_class = ObjFactory.parser(
            model_xbrl, None
        )
        xmlDoc = etree.parse(root_string, parser=_parser)
        modelDoc = ModelRssObject(
            model_xbrl, ModelDocument.Type.RSSFEED, xmlDocument=xmlDoc
        )
        model_xbrl.modelDocument = modelDoc
        modelDoc.rssFeedDiscover(xmlDoc.getroot())
        gettext.install("arelle")
        for item in modelDoc.rssItems:
            item.init(modelDoc)

        return model_xbrl

    def save_filings(
        self,
        filings_list: Query[Any],
        filename: str,
        type_: str,
        title: str | None = None,
        description: str | None = None,
        return_object: bool = False,
    ) -> tuple[str, ModelDocument.ModelDocument | list[dict[str, Any]] | None]:
        """Saves a list of filings in the specified format in
        `type` "json" or "rss", saved to `filename`, if file name == 'memory'
        file will not be saved"""
        result: ModelDocument.ModelDocument | list[
            dict[str, Any]
        ] | None = None
        result_len = 0
        if type_ == "json":
            result = []
            for filing in filings_list:
                filing_dict = filing.to_dict()
                filing_dict["files"] = [x.to_dict() for x in filing.files]
                result.append(filing_dict)
            result_len = len(result)
            if filename != "memory":
                with open(filename, "w", encoding="utf-8") as _fh:
                    json.dump(result, _fh, default=str)
        elif type_ == "rss":
            model_xbrl = self._make_rss_feed(
                filings_list, filename, title, description
            )
            assert isinstance(
                model_xbrl.modelDocument, ModelDocument.ModelDocument
            )
            result_len = len(getattr(model_xbrl.modelDocument, "rssItems", []))
            result = model_xbrl.modelDocument
            result.filepath = os.path.abspath(filename)
            xml_document = result.xmlDocument
            if filename != "memory":
                with open(filename, "w", encoding="utf-8") as _fh:
                    XmlUtil.writexml(_fh, xml_document, encoding="utf-8")
        else:
            raise XIDBException(
                constants.ERR_BAD_TYPE,
                f"Don't know what to do with {type_}...",
            )
        self.cntlr.addToLog(
            f"Created {filename} for {result_len:,} filings.",
            **log_template("info", self.database),
        )
        return (filename, result if return_object else None)

    def get_all_industry_tree(
        self, industry_classification: str
    ) -> dict[str, Any]:
        """Returns dict resembling industry hierarchy for the selected
        classification, dict keys are
        `depth|industry_code|industry_description`.
        `industry_classification` options are:
        - 'SEC' (only relevant system that is linked to filing)
        - 'SIC'
        - 'NAICS'
        """

        def child_industry(_industries: Query[Any]) -> dict[str, Any]:
            return {
                "|".join(
                    (
                        str(i.depth),
                        str(i.industry_code),
                        i.industry_description,
                    )
                ): child_industry(i.children)
                if len(i.children) > 0
                else None
                for i in _industries
            }

        industry_dict: dict[str, Any] = {}
        with Session(self.engine) as session:
            industry = (
                session.query(SEC.SecIndustry)
                .filter(
                    SEC.SecIndustry.depth == 1,
                    SEC.SecIndustry.industry_classification
                    == industry_classification,
                )
                .order_by(SEC.SecIndustry.industry_code)
            )
            for i in industry:
                industry_dict[
                    "|".join(
                        (
                            str(i.depth),
                            str(i.industry_code),
                            i.industry_description,
                        )
                    )
                ] = child_industry(i.children)
        return industry_dict

    def search_filings(
        self,
        filing_system: Literal["sec", "esef"],
        publication_date_from: str | None = None,
        publication_date_to: str | None = None,
        report_date_from: str | None = None,
        report_date_to: str | None = None,
        filing_number: str | None = None,
        filer_identifier: str | None = None,
        industry_code: str | None = None,
        industry_code_tree: str | None = None,
        industry_name: str | None = None,
        form_type: str | None = None,
        filer_name: str | None = None,
        filer_ticker_symbol: str | None = None,
        esef_country_alpha2: str | None = None,
        esef_country_name: str | None = None,
        random: bool = False,
        limit: int = 100,
    ) -> Query[Any]:
        """Search filings according to the specified fields
        Filing system must be one of 'esef' or 'sec'.
        Fields values should be a string of the value to look up or
        comma separated strings if multiple values should be matched.
        To match a record all fields with values are used in an AND
        operator, if a field has comma separated string, this is treated
        as an OR operator, for example:
        match publication_date_from AND publication_date_to AND
        (ticker_symbol like '%abc%' OR ticker_symbol like '%def%')).
        By default, query is limited to 100 results, change `limit`
        or set to None to get all matches.
        """
        if filing_system.lower() not in ("esef", "sec"):
            raise XIDBException(
                constants.ERR_BAD_SEARCH_PARAM,
                "filing_system must be one of `esef` or `sec`",
            )

        def check_date(param: Any, param_name: str) -> bool:
            date_format_msg = (
                "param {} must be a valid date in formate 2022-10-21"
            )
            try:
                parser.parse(param)
            except Exception as err:
                raise XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    date_format_msg.format(param_name),
                ) from err
            return True

        filter_list = []
        filings_table_q: Query[Any] = {
            "esef": (
                Query(ESEF.EsefFiling)  # type: ignore[arg-type]
                .select_from(ESEF.EsefFiling)
                .join(
                    ESEF.EsefEntity,
                    ESEF.EsefFiling.entity_lei == ESEF.EsefEntity.entity_lei,
                    isouter=True,
                )
                .join(
                    BASE_M.Location,
                    ESEF.EsefFiling.country == BASE_M.Location.alpha_2,
                    isouter=True,
                )
                .distinct()
            ),
            "sec": (
                Query(SEC.SecFiling)  # type: ignore[arg-type]
                .select_from(SEC.SecFiling)
                .join(
                    SEC.SecCikTickerMapping,
                    SEC.SecFiling.cik_number
                    == SEC.SecCikTickerMapping.cik_number,
                    isouter=True,
                )
                .join(
                    SEC.SecIndustry,
                    and_(
                        SEC.SecIndustry.industry_classification == "SEC",
                        SEC.SecFiling.assigned_sic
                        == SEC.SecIndustry.industry_code,
                    ),
                    isouter=True,
                )
                .distinct()
            ),
        }[filing_system]

        filing_number_col = {
            "esef": ESEF.EsefFiling.filing_key,
            "sec": SEC.SecFiling.accession_number,
        }[filing_system]
        filer_identifier_col = {
            "esef": ESEF.EsefFiling.entity_lei,
            "sec": SEC.SecFiling.cik_number,
        }[filing_system]
        filer_name_col = {
            "esef": ESEF.EsefEntity.lei_legal_name,
            "sec": SEC.SecFiling.company_name,
        }[filing_system]
        pub_date_from_col = {
            "esef": ESEF.EsefFiling.date_added,
            "sec": SEC.SecFiling.pub_date,
        }[filing_system]
        report_date_from_col = {
            "esef": ESEF.EsefFiling.report_date,
            "sec": SEC.SecFiling.period,
        }[filing_system]

        if publication_date_from is not None:
            checked = check_date(
                publication_date_from, "publication_date_from"
            )
            if checked:
                filter_list.append(
                    pub_date_from_col >= parser.parse(publication_date_from)
                )

        if publication_date_to is not None:
            checked = check_date(publication_date_to, "publication_date_to")
            if checked:
                filter_list.append(
                    pub_date_from_col
                    < parser.parse(publication_date_to)
                    + datetime.timedelta(days=1)
                )

        if report_date_from is not None:
            checked = check_date(report_date_from, "report_date_from")
            if checked:
                filter_list.append(
                    report_date_from_col >= parser.parse(report_date_from)
                )

        if report_date_to is not None:
            checked = check_date(report_date_to, "report_date_to")
            if checked:
                filter_list.append(
                    report_date_from_col
                    < parser.parse(report_date_to) + datetime.timedelta(days=1)
                )

        if filing_number is not None:
            if not isinstance(filing_number, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "filing_number should be a string of one or more comma "
                    "separated FULL and EXISTING SEC accession number(s) or "
                    "ESEF index filing(s) key(s)",
                )
            filter_list.append(
                filing_number_col.in_(
                    [x.strip() for x in filing_number.split(",")]
                )
            )

        if filer_identifier is not None:
            if not isinstance(filer_identifier, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "filer_identifier should be a string of one or more comma "
                    "separated FULL and EXISTING SEC cik number(s) or ESEF "
                    "index lei(s)",
                )
            filter_list.append(
                func.lower(filer_identifier_col).in_(
                    [x.strip() for x in filer_identifier.split(",")]
                )
            )

        if industry_code is not None:
            if not isinstance(industry_code, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "industry_code should be a string of one or more comma "
                    "separated FULL and EXISTING SEC Industry Classification "
                    "Code(s) this search option is valid for SEC filings only",
                )
            if filing_system == "sec" and industry_code_tree is None:
                filter_list.append(
                    SEC.SecFiling.assigned_sic.in_(
                        [int(x.strip()) for x in industry_code.split(",")]
                    )
                )
            elif filing_system == "esef" or industry_code_tree is not None:
                self.cntlr.addToLog(
                    "Search parameter `industry_code` is ignored for "
                    "ESEF searches, or if `industry_code_tree` parameter "
                    "has value.",
                    **log_template("warning", self.database),
                )

        if industry_name is not None:
            if not isinstance(industry_name, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "industry_name should be a string of one or more comma "
                    "separated full or partial SEC Industry Classification "
                    "description, this search option is valid for SEC "
                    "filings only",
                )
            if (
                filing_system == "sec"
                and industry_code is None
                and industry_code_tree is NotImplemented
            ):
                filter_list.append(
                    or_(
                        *[
                            func.lower(
                                SEC.SecIndustry.industry_description
                            ).like("%" + x.lower().strip() + "%")
                            for x in industry_name.split(",")
                        ]
                    )
                )
            elif (
                filing_system == "esef"
                or industry_code is not None
                or industry_code_tree is not None
            ):
                self.cntlr.addToLog(
                    "Search parameter `industry_name` is ignored for "
                    "ESEF searches or when any of `industry_core` or "
                    "`industry_code_tree` parameters has value.",
                    **log_template("warning", self.database),
                )

        if industry_code_tree is not None:
            if not isinstance(industry_code_tree, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "industry_code_tree should be a string of one or more "
                    "comma separated FULL and EXISTING SEC Industry "
                    "Classification Code(s) this search option is "
                    "valid for SEC filings only",
                )
            if filing_system == "sec":
                _param = [
                    int(x.strip()) for x in industry_code_tree.split(",")
                ]
                industry_tree = self.get_an_industry_tree(
                    industry_codes=_param
                )
                industry_tree_codes = [x[0] for x in industry_tree]
                filter_list.append(
                    SEC.SecFiling.assigned_sic.in_(industry_tree_codes)
                )
            elif filing_system == "esef":
                self.cntlr.addToLog(
                    "Search parameter `industry_code_tree` is ignored for "
                    "ESEF searches.",
                    **log_template("warning", self.database),
                )

        if esef_country_alpha2 is not None:
            if not isinstance(esef_country_alpha2, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "esef_country_alpha2 should be a string of one or more "
                    "comma separated FULL and EXISTING alpha-2 country "
                    "code(s) this search option is valid for ESEF "
                    "filings only",
                )
            if filing_system == "esef":
                filter_list.append(
                    ESEF.EsefFiling.country.in_(
                        [x.strip() for x in esef_country_alpha2.split(",")]
                    )
                )
            elif filing_system == "sec":
                self.cntlr.addToLog(
                    "Search parameter `esef_country_alpha2` is ignored for "
                    "SEC searches.",
                    **log_template("warning", self.database),
                )

        if esef_country_name is not None:
            if not isinstance(esef_country_name, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "industry_name should be a string of one or more comma "
                    "separated full or partial SEC Industry Classification "
                    "description, this search option is valid for SEC "
                    "filings only",
                )
            if filing_system == "esef" and esef_country_alpha2 is None:
                filter_list.append(
                    or_(
                        *[
                            func.lower(BASE_M.Location.country).like(
                                "%" + x.lower().strip() + "%"
                            )
                            for x in esef_country_name.split(",")
                        ]
                    )
                )
            elif filing_system == "sec" or esef_country_alpha2 is not None:
                self.cntlr.addToLog(
                    "Search parameter `esef_country_name` is ignored for "
                    "SEC searches or when `esef_country_alpha2` has value.",
                    **log_template("warning", self.database),
                )

        if form_type is not None:
            if not isinstance(form_type, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "from_type should be a string of one or more comma "
                    "separated full/partial SEC form type (10-K, 10-Q, ...) "
                    "this search option is valid for SEC filings "
                    "only, ESEF has only one type (AFR).",
                )
            if filing_system == "sec":
                filter_list.append(
                    or_(
                        *[
                            func.lower(SEC.SecFiling.form_type).like(
                                "%" + x.lower().strip() + "%"
                            )
                            for x in form_type.split(",")
                        ]
                    )
                )
            elif filing_system == "esef":
                self.cntlr.addToLog(
                    "Search parameter `form_type` is ignored for ESEF search"
                    " Only Annual Financial Report (AFR) is available.",
                    **log_template("warning", self.database),
                )

        if filer_name is not None:
            if not isinstance(filer_name, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "filer_name should be a string of one or more "
                    "comma separated full or partial filer/company name(s)",
                )
            filter_list.append(
                or_(
                    *[
                        func.lower(filer_name_col).like(
                            "%" + x.lower().strip() + "%"
                        )
                        for x in filer_name.split(",")
                    ]
                )
            )

        if filer_ticker_symbol is not None:
            if not isinstance(filer_ticker_symbol, str):
                raise constants.XIDBException(
                    constants.ERR_BAD_SEARCH_PARAM,
                    "filer_ticker_symbol should be a string of one or "
                    "more comma separated full or partial ticker symbol(s), "
                    "valid for SEC filings only.",
                )
            if filing_system == "sec":
                filter_list.append(
                    or_(
                        *[
                            func.lower(
                                SEC.SecCikTickerMapping.ticker_symbol
                            ).like("%" + x.lower().strip() + "%")
                            for x in filer_ticker_symbol.split(",")
                        ]
                    )
                )
            elif filing_system == "esef":
                self.cntlr.addToLog(
                    "Search parameter `ticker_symbol` is "
                    "ignored for ESEF search",
                    **log_template("warning", self.database),
                )

        result_qry = filings_table_q
        if len(filter_list) > 0:
            result_qry = result_qry.filter(and_(*filter_list))
        if random:
            if self.product == "mssql":  # type: ignore[comparison-overlap]
                self.cntlr.addToLog(
                    "Random selection does not work for MS SQL server "
                    "ignoring random option."
                )
            else:
                assert isinstance(self.product, str)
                result_qry = result_qry.order_by(
                    random_function[self.product]()
                )
                if limit is None:
                    # mandatory to have a limit if random
                    limit = 20
        if limit:
            result_qry = result_qry.limit(limit)
        return result_qry

    def get_an_industry_tree(
        self,
        industry_codes: list[int] | None = None,
        industry_classification: Literal["SEC", "SIC", "NAICS"] = "SEC",
        verbose: bool = False,
        style: Literal["indented", "csv"] = "indented",
    ) -> list[tuple[int, str, str, int]]:
        """Get all child industries starting for one or more
        industry code(s)"""

        def get_industry_row(
            inst: SEC.SecIndustry,
            tree_list: list[tuple[int, str, str, int]],
            verbose: bool,
            style: Literal["indented", "csv"],
        ) -> None:
            row = (
                int(inst.industry_code),
                str(inst.industry_description),
                str(inst.industry_classification),
                int(inst.depth),
            )
            tree_list.append(row)
            if verbose:
                if style == "csv":
                    print(f'{row[0]},"{row[1]}","{row[2]}",{row[3]}')
                else:
                    print(
                        "\t" * (row[-1] - 1),
                        f"{row[0]} - {row[1]} ({row[2]} depth {row[-1]})",
                    )
            if len(inst.children) > 0:
                for child in inst.children:
                    get_industry_row(child, tree_list, verbose, style)

        filter_list = [
            SEC.SecIndustry.industry_classification
            == industry_classification.upper(),
        ]
        if industry_codes is not None:
            assert isinstance(industry_codes, list)
            filter_list.append(
                SEC.SecIndustry.industry_code.in_(industry_codes)
            )
        else:
            # make sure depth is 1 if we are getting everything
            filter_list.append(SEC.SecIndustry.depth == 1)
        tree_list: list[tuple[int, str, str, int]] = []
        if verbose:
            if style == "csv":
                print(
                    ",".join(
                        [
                            "industry_code",
                            "description",
                            "classification",
                            "depth",
                        ]
                    )
                )
        with Session(self.engine) as session:
            industry_table = session.query(SEC.SecIndustry).filter(
                and_(*filter_list)
            )
            for x in industry_table:
                get_industry_row(x, tree_list, verbose, style)
        return tree_list

    def list_countries_info(
        self, verbose: bool = False
    ) -> list[tuple[str, str, str | None, float, float]]:
        """Returns a list of the available country information"""
        countries: list[tuple[str, str, str | None, float, float]] = []
        if verbose:
            print(
                ",".join(
                    [
                        "code",
                        "country_name",
                        "state_province",
                        "longitude",
                        "latitude",
                    ]
                )
            )
        with Session(self.engine) as session:
            locations = session.query(BASE_M.Location)
            for loc in locations:
                row = (
                    str(loc.code),
                    str(loc.country),
                    str(loc.state_province)
                    if loc.state_province is not None
                    else loc.state_province,
                    float(loc.longitude),
                    float(loc.latitude),
                )
                countries.append(row)
                if verbose:
                    print(
                        f'"{row[0]}","{row[1]}","{row[2] if row[2] else ""}"'
                        f",{row[3]},{row[4]}"
                    )
        return countries
