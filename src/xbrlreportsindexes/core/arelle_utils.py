"""Extending Arelle Cntlr class and few other utilities related to arelle"""
from __future__ import annotations

import datetime
import sys
from logging import LogRecord
from typing import Any
from typing import Optional


try:
    from arelle.Cntlr import Cntlr, LogToBufferHandler, LogFormatter
except ModuleNotFoundError as exc:
    raise Exception("Please add path to arelle to python path") from exc

RSS_DB_LOG_HANDLER_NAME = "rss-db-log-handler"


class CntlrPy(Cntlr):
    """Extends arelle.Cntlr.Cntlr"""

    def __init__(
        self,
        hasGui: bool = False,
        logFileName: Optional[str] = None,
        logFileMode: Optional[str] = None,
        logFileEncoding: Optional[str] = None,
        logFormat: Optional[str] = None,
    ) -> None:
        super().__init__(
            hasGui, logFileName, logFileMode, logFileEncoding, logFormat
        )
        self.startLogging(logFileName="logToBuffer")

    def showStatus(self, message: str, clearAfter: Any = None) -> None:
        """print out messages"""
        print(message, end="\r")


class IndexDBLogHandler(LogToBufferHandler):
    """combine log to buffer with log to print"""

    def __init__(
        self,
        logOutput: str | None,
        verbose: bool = True,
        logFormat: Optional[str] = None,
        is_test: bool = False,
    ) -> None:
        super().__init__()
        self.is_test: bool = is_test
        self.set_name(RSS_DB_LOG_HANDLER_NAME)
        self.verbose = verbose
        self.logFile = sys.stdout
        if logOutput == "logToStdErr":
            self.logFile = sys.stderr

        self.setFormatter(
            LogFormatter(
                logFormat
                or f"%(asctime)s [{'TEST:' if is_test else ''}%"
                f"(messageCode)s] %(message)s - %(file)s"
            )
        )

    def emit(self, logRecord: LogRecord) -> None:
        """Emit implementation"""
        self.logRecordBuffer.append(logRecord)
        logEntry = self.format(logRecord)
        if self.verbose:
            file = sys.stderr if self.logFile else None
            try:
                print(logEntry, file=file)
            except UnicodeEncodeError:
                print(
                    (
                        logEntry.encode(
                            sys.stdout.encoding, "backslashreplace"
                        ).decode(sys.stdout.encoding, "strict")
                    ),
                    file=file,
                )

    def get_log_records(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Extract log records"""
        log_buffer = self.logRecordBuffer
        processing_log = []
        action_log = []
        for rec in log_buffer:
            timestamp = datetime.datetime.fromtimestamp(rec.created).replace(
                microsecond=0
            )
            task = (
                getattr(rec, "messageCode", "").split(".")[1]
                if len(getattr(rec, "messageCode", "").split(".")) > 1
                else None
            )
            log_row = {
                "timestamp_at": timestamp,
                "task": task,
                "message": rec.message,
                "time_taken": None,
                "subject": None,
            }
            for ref in getattr(rec, "refs", []):
                if ref.get("time", False):  # check if we have time in this ref
                    log_row["time_taken"] = ref["time"]
                if ref.get("href", False):  # check if we have file in this ref
                    log_row["subject"] = ref.get("href", None)
                if isinstance(ref, dict) and "stats" in ref:
                    for stat_row in ref["stats"]:
                        action_row = {
                            "task": task,
                            "timestamp_at": stat_row[0],
                            "feed_id": stat_row[1],
                            "table_name": stat_row[2],
                            "action": stat_row[3],
                            "rowcount": stat_row[4],
                            "time_taken": stat_row[5],
                            "is_committed": stat_row[6],
                        }
                        action_log.append(action_row)

            processing_log.append(log_row)

        return processing_log, action_log
