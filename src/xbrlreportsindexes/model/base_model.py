"""Base database model foundation to all other models,
includes tables necessary to manage the database, and
tables with static dataset.
"""
from __future__ import annotations

import datetime
import re
from re import Pattern
from typing import Any
from typing import Union

from sqlalchemy import BOOLEAN
from sqlalchemy import Column
from sqlalchemy import false
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declarative_mixin
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import TableClause
from sqlalchemy.types import NullType
from sqlalchemy.types import (
    BigInteger,
    DateTime,
    Integer,
    Numeric,
    SmallInteger,
    String,
)

find_caps: Pattern[str] = re.compile(r"[a-zA-Z][^A-Z]*")


def replace_caps(word: str) -> str:
    """Replace caps with snake case"""
    split_name = find_caps.findall(word)
    lower_name = "_".join([w.lower() for w in split_name])
    return lower_name


meta: MetaData = MetaData()


class _Base:
    """Base model with common utilities"""

    __name__: str
    __table__: Union[Table, TableClause]

    @declared_attr
    def __tablename__(self) -> str:
        """Convert class name to snake_case"""
        return replace_caps(self.__name__)

    @classmethod
    def cols_names(cls) -> list[str]:
        """Get column names"""
        table: Table = getattr(cls, "__table__", Table(cls.__tablename__, cls.__table__.metadata))
        columns: Column[Any] = table.columns
        return columns.keys()

    def to_dict(self) -> dict[str, Any]:
        """Convert instance to dict"""
        table = getattr(self, "__table__", Table())
        result_dict = {}
        for col in table.columns.keys():
            result_dict[col] = getattr(self, col, None)
        return result_dict


Base: type = declarative_base(cls=_Base, metadata=meta)


@declarative_mixin
class FeedIdColMixin:
    """Adds feed id column"""

    @property
    def __tablename__(self) -> str:
        ...

    @declared_attr
    def feed_id(self) -> Mapped[int]:
        """Creates feed_id column"""
        if self.__tablename__ != "sec_feed":
            return mapped_column(
                BigInteger(),
                ForeignKey(
                    "sec_feed.feed_id", onupdate="RESTRICT", ondelete="CASCADE"
                ),
                autoincrement=False,
                nullable=False,
            )
        return Column(
            BigInteger(),
            primary_key=True,
            autoincrement=False,
            nullable=False,
        )


@declarative_mixin
class FilingIdColMixin:
    """adds filing id column"""

    @property
    def __tablename__(self) -> str:
        ...

    @declared_attr
    def filing_id(self) -> Mapped[int]:
        """Create filing id column"""
        if self.__tablename__ != "sec_filing":
            return mapped_column(
                BigInteger(),
                ForeignKey(
                    "sec_filing.filing_id",
                    onupdate="RESTRICT",
                    ondelete="CASCADE",
                ),
                autoincrement=False,
                nullable=False,
            )
        return Column(
            BigInteger(),
            primary_key=True,
            autoincrement=False,
            nullable=False,
        )


@declarative_mixin
class CreatedUpdatedAtColMixin:
    """Adds created at timestamp column"""

    @declared_attr
    def created_updated_at(self) -> Mapped[datetime.datetime]:
        """Creates created at col"""
        return Column(
            DateTime(timezone=True),
            server_default=func.CURRENT_TIMESTAMP(),
            onupdate=datetime.datetime.now,
        )


@declarative_mixin
class LogTablesMixin:
    """Common columns for log tables"""

    @declared_attr
    def log_id(self) -> Mapped[int]:
        """log id column"""
        return Column(
            BigInteger().with_variant(Integer, "sqlite"),
            nullable=False,
            primary_key=True,
            autoincrement=True,
        )

    @declared_attr
    def timestamp_at(self) -> Mapped[datetime.datetime]:
        """timestamp column"""
        return Column(DateTime(timezone=True), nullable=False)

    @declared_attr
    def task(self) -> Mapped[str]:
        """Task column"""
        return Column(String(), nullable=True)

    @declared_attr
    def time_taken(self) -> Mapped[float]:
        """time taken column"""
        return Column(Numeric(), nullable=True)


class Location(Base, CreatedUpdatedAtColMixin):
    """Locations based on SEC/EDGAR coding, in addition to alpha 2 and 3."""

    __table_args__ = {"comment": "all"}
    # columns
    code = Column(String(), nullable=False, primary_key=True)
    latitude = Column(Numeric(), nullable=True)
    longitude = Column(Numeric(), nullable=True)
    country = Column(String(), nullable=True)
    alpha_2 = Column(String(2), nullable=True)
    alpha_3 = Column(String(3), nullable=True)
    numeric = Column(String(3), nullable=True)
    state_province = Column(String(), nullable=True)
    location_fix = Column(String(), nullable=True)


class ProcessingLog(Base, LogTablesMixin):
    """Stores log messages produced during processing"""

    __table_args__ = {"comment": "all"}
    message = Column(String(), nullable=True)
    subject = Column(String(), nullable=True)
    task_id = Column(BigInteger(), nullable=True)


class ActionLog(Base, LogTablesMixin):
    """Tracks tables bulk inserts and deletes"""

    __table_args__ = {"comment": "all"}
    feed_id = Column(
        BigInteger(),
        nullable=False,
        server_default=text("999999"),
    )
    action = Column(String(), nullable=True)
    table_name = Column(String(), nullable=True)
    rowcount = Column(Integer(), nullable=True)
    is_committed = Column(BOOLEAN(), nullable=True)
    task_id = Column(BigInteger(), nullable=True)


class LastUpdate(Base, CreatedUpdatedAtColMixin):
    """Last database update date time"""

    __table_args__ = {"comment": "all"}
    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    )
    task = Column(String(), nullable=False)
    last_updated = Column(DateTime(timezone=True), nullable=False)


class TaskTracker(Base, CreatedUpdatedAtColMixin):
    """Tracks tasks processed, and helps in blocking
    other tasks from starting if there is a task
    currently running"""

    __table_args__ = {"comment": "all"}
    task_id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        nullable=False,
        autoincrement=True,
    )
    process_id = Column(BigInteger())
    # one of initialize-db, update-feeds, update-filers, refresh-tables
    task_name = Column(String(), nullable=False)
    # is properly closed and everything handled
    is_closed = Column(BOOLEAN, nullable=False, server_default=false())
    is_completed = Column(BOOLEAN, nullable=False, server_default=false())
    is_interrupted = Column(BOOLEAN, nullable=False, server_default=false())
    task_parameters = Column(String(), nullable=False)
    started_at = Column(
        DateTime(timezone=True),
        nullable=True,
        default=datetime.datetime.now,
    )
    ended_at: datetime.datetime | Column[NullType] = Column(
        DateTime(timezone=True)
    )
    time_taken = Column(Numeric())
    total_items: Mapped[int] = Column(
        Integer(), default=text("0")
    )
    completed_items: Mapped[int] = Column(
        Integer(), default=text("0")
    )
    successful_items: Mapped[int] = Column(
        Integer(), default=text("0")
    )
    failed_items: Mapped[int] = Column(
        Integer(), default=text("0")
    )
    task_notes: Column[NullType] = Column(String())
