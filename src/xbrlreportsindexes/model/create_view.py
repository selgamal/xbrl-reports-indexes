"""From https://github.com/sqlalchemy/sqlalchemy/wiki/Views
with very minor change"""
from __future__ import annotations

from typing import Any
from typing import Union

import sqlalchemy as sa
from sqlalchemy import Table
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import Select
from sqlalchemy.sql import table
from sqlalchemy.sql import TableClause
from sqlalchemy.sql.base import DedupeColumnCollection
from sqlalchemy.sql.compiler import Compiled


class CreateView(DDLElement):
    """Create view class wrapper for DDLELement Class"""

    def __init__(self, name: str, selectable: Select) -> None:
        self.name = name
        self.selectable: Select = selectable


class DropView(DDLElement):
    """Drop view class wrapper for DDLElement"""

    def __init__(self, name: str) -> None:
        self.name = name


@compiler.compiles(CreateView)  # type: ignore[misc]
def _create_view(element: CreateView, compiler_: Compiled, **kw: Any) -> str:
    """Creates view implementation"""
    view_sql = compiler_.sql_compiler.process(
        element.selectable, literal_binds=True
    )
    return f"CREATE VIEW {element.name} AS {view_sql}"


@compiler.compiles(DropView)  # type: ignore[misc]
def _drop_view(element: DropView, compiler_: Compiled, **kw: str) -> str:
    """Drop view implementation"""
    return f"DROP VIEW {element.name}"


def view_exists(
    ddl: Union[DropView, CreateView],
    target: str,
    connection: sa.engine.Connection,
    **kw: Any,
) -> bool:
    """Exists implementation"""
    return ddl.name in sa.inspect(connection).get_view_names()


def ok_to_create_view(
    ddl: CreateView, target: str, connection: sa.engine.Connection, **kw: Any
) -> bool:
    """Before create view make sure from tables exist"""
    _view_tables: list[Table] = getattr(
        ddl.selectable, "columns_clause_froms", []
    )
    view_table_froms: set[str] = set()
    if view_table_froms:
        view_table_froms = {x.key for x in _view_tables}
    inspector = sa.inspect(connection)
    existing_tables = set(inspector.get_table_names())
    if len(view_table_froms - existing_tables):
        return False
    return not view_exists(ddl, target, connection, **kw)


def view(
    name: str, metadata: MetaData, selectable: Select, mock: bool = False
) -> TableClause:
    """Function to create view"""
    table_ = table(name)

    columns: DedupeColumnCollection[Any] = getattr(
        table_, "_columns", DedupeColumnCollection()
    )
    columns._populate_separate_keys(
        col._make_proxy(table_) for col in selectable.selected_columns
    )

    if not mock:
        sa.event.listen(
            metadata,
            "after_create",
            CreateView(name, selectable).execute_if(
                callable_=ok_to_create_view  # type: ignore[arg-type]
            ),
        )
        sa.event.listen(
            metadata,
            "before_drop",
            DropView(name).execute_if(
                callable_=view_exists  # type: ignore[arg-type]
            ),
        )
    return table_


# for usage demonstration only as in the link above
# if __name__ == "__main__":
#     engine = sa.create_engine("sqlite://", echo=True)
#     metadata = sa.MetaData()
#     stuff = sa.Table(
#         "stuff",
#         metadata,
#         sa.Column("id", sa.Integer, primary_key=True),
#         sa.Column("data", sa.String(50)),
#     )

#     more_stuff = sa.Table(
#         "more_stuff",
#         metadata,
#         sa.Column("id", sa.Integer, primary_key=True),
#         sa.Column("stuff_id", sa.Integer, sa.ForeignKey("stuff.id")),
#         sa.Column("data", sa.String(50)),
#     )

#     # the .label() is to suit SQLite which needs explicit label names
#     # to be given when creating the view
#     # See http://www.sqlite.org/c3ref/column_name.html
#     stuff_view = view(
#         "stuff_view",
#         metadata,
#         sa.select(
#             stuff.c.id.label("id"),
#             stuff.c.data.label("data"),
#             more_stuff.c.data.label("moredata"),
#         )
#         .select_from(stuff.join(more_stuff))
#         .where(stuff.c.data.like(("%orange%"))),
#     )

#     assert stuff_view.primary_key == [stuff_view.c.id]

#     with engine.begin() as conn:
#         metadata.create_all(conn)

#     with engine.begin() as conn:
#         conn.execute(
#             stuff.insert(),
#             [
#                 {"data": "apples"},
#                 {"data": "pears"},
#                 {"data": "oranges"},
#                 {"data": "orange julius"},
#                 {"data": "apple jacks"},
#             ],
#         )

#         conn.execute(
#             more_stuff.insert(),
#             [
#                 {"stuff_id": 3, "data": "foobar"},
#                 {"stuff_id": 4, "data": "foobar"},
#             ],
#         )

#     with engine.connect() as conn:
#         assert conn.execute(
#             sa.select(stuff_view.c.data, stuff_view.c.moredata)
#         ).all() == [("oranges", "foobar"), ("orange julius", "foobar")]

#     # illustrate ORM usage
#     from sqlalchemy.orm import declarative_base
#     from sqlalchemy.orm import Session

#     Base = declarative_base(metadata=metadata)

#     class MyStuff(Base):
#         __table__ = stuff_view

#         def __repr__(self):
#             return f"MyStuff({self.id!r}, {self.data!r}, {self.moredata!r})"

#     with Session(engine) as s:
#         print(s.query(MyStuff).all())
