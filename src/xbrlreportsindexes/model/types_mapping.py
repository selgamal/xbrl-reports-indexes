"""Types variants for backends"""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Union

from sqlalchemy import func
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql.functions import _FunctionGenerator
from sqlalchemy.types import BigInteger
from sqlalchemy.types import Date
from sqlalchemy.types import FLOAT
from sqlalchemy.types import Integer
from sqlalchemy.types import NUMERIC
from sqlalchemy.types import Text
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import Variant

BIGINT_TYPE: dict[str, Any] = {
    "postgres": postgresql.BIGINT(),
    "sqlite": sqlite.INTEGER(),
    "mysql": mysql.BIGINT(),
    "oracle": oracle.NUMBER(19),
    "mssql": mssql.BIGINT(),
}
INTEGER_TYPE: dict[str, Any] = {
    "postgres": postgresql.INTEGER(),
    "sqlite": sqlite.INTEGER(),
    "mysql": mysql.INTEGER(),
    "oracle": oracle.NUMBER(19),
    "mssql": mssql.INTEGER(),
}
NUMERIC_TYPE: dict[str, Any] = {
    "postgres": postgresql.NUMERIC(),
    "sqlite": sqlite.NUMERIC(),
    "mysql": mysql.NUMERIC(),
    "oracle": oracle.NUMBER(),
    "mssql": mssql.NUMERIC(),
}
DATE_TYPE: dict[str, Any] = {
    "sqlite": sqlite.DATE(),
    "postgres": postgresql.DATE(),
    "mysql": mysql.DATE(),
    "oracle": oracle.DATE(),
    "mssql": mssql.DATE(),
}
TEXT_TYPE: dict[str, Any] = {
    "sqlite": sqlite.TEXT(),
    "postgres": postgresql.TEXT(),
    "mysql": mysql.TEXT(),
    "oracle": oracle.VARCHAR(),
    "mssql": mssql.TEXT(),
}
TIMESTAMP_TYPE: dict[str, Any] = {
    "sqlite": sqlite.TIMESTAMP(),
    "postgres": postgresql.TIMESTAMP(),
    "mysql": mysql.TIMESTAMP(),
    "oracle": oracle.TIMESTAMP(),
    "mssql": mssql.TIMESTAMP(),
}
TIMESTAMPTZ_TYPE: dict[str, Any] = {
    "sqlite": sqlite.TIMESTAMP(timezone=True),
    "postgres": postgresql.TIMESTAMP(timezone=True),
    "mysql": mysql.TIMESTAMP(timezone=True),
    "oracle": oracle.TIMESTAMP(timezone=True),
    "mssql": mysql.TIMESTAMP(timezone=True),
}
CHARVARYING_TYPE: dict[str, Any] = {
    "sqlite": sqlite.VARCHAR(),
    "postgres": postgresql.VARCHAR(),
    "mysql": mysql.VARCHAR(),
    "oracle": oracle.VARCHAR(),
    "mssql": mssql.VARCHAR(),
}
FLOAT_TYPE: dict[str, Any] = {
    "sqlite": sqlite.FLOAT(),
    "postgres": postgresql.FLOAT(),
    "mysql": mysql.FLOAT(),
    "oracle": oracle.FLOAT(),
}

random_function: dict[str, Union[Callable[[], str], _FunctionGenerator]] = {
    "postgres": func.random,
    "sqlite": func.random,
    "mysql": func.rand,
    "oracle": lambda: "dbms_random.value",
}

# Bigint_type: Variant[type] = Variant(BigInteger(), BIGINT_TYPE)
# Integer_type: Variant[type] = Variant(Integer(), BIGINT_TYPE)
# Date_type: Variant[type] = Variant(Date(), DATE_TYPE)
# Text_type: Variant[type] = Variant(Text(), TEXT_TYPE)
# Timestamp_type: Variant[type] = Variant(TIMESTAMP(), TIMESTAMP_TYPE)
# Timestamptz_type: Variant[type] = Variant(
#     TIMESTAMP(timezone=True), TIMESTAMPTZ_TYPE
# )
# Char_varying_type: Variant[type] = Variant(VARCHAR(), CHARVARYING_TYPE)
# Numeric_type: Variant[type] = Variant(NUMERIC(), NUMERIC_TYPE)
# Float_type: Variant[type] = Variant(FLOAT(), FLOAT_TYPE)
