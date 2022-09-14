import pymysql
import sqlite3
from _typeshed import Incomplete

TRACESQLFILE: Incomplete

def noop(*args, **kwargs) -> None: ...

class NoopException(Exception): ...

hasPostgres: bool
pgConnect: Incomplete
pgOperationalError: Incomplete
pgProgrammingError: Incomplete
pgInterfaceError: Incomplete
pgConnect = noop
pgOperationalError = NoopException
pgProgrammingError = NoopException
pgInterfaceError = NoopException
hasMySql: bool
mysqlConnect: Incomplete
mysqlProgrammingError = pymysql.ProgrammingError
mysqlInterfaceError = pymysql.InterfaceError
mysqlInternalError = pymysql.InternalError
mysqlConnect = noop
mysqlProgrammingError = NoopException
mysqlInterfaceError = NoopException
mysqlInternalError = NoopException
hasOracle: bool
oracleConnect: Incomplete
oracleDatabaseError: Incomplete
oracleInterfaceError: Incomplete
oracleNCLOB: Incomplete
oracleConnect = noop
oracleDatabaseError = NoopException
oracleInterfaceError = NoopException
oracleCLOB: Incomplete
hasMSSql: bool
mssqlConnect: Incomplete
mssqlOperationalError: Incomplete
mssqlProgrammingError: Incomplete
mssqlInterfaceError: Incomplete
mssqlInternalError: Incomplete
mssqlDataError: Incomplete
mssqlIntegrityError: Incomplete
mssqlConnect = noop
mssqlOperationalError = NoopException
mssqlProgrammingError = NoopException
mssqlInterfaceError = NoopException
mssqlInternalError = NoopException
mssqlDataError = NoopException
mssqlIntegrityError = NoopException
hasSQLite: bool
sqliteConnect = sqlite3.connect
sqliteParseDecltypes: Incomplete
sqliteOperationalError = sqlite3.OperationalError
sqliteProgrammingError = sqlite3.ProgrammingError
sqliteInterfaceError = sqlite3.InterfaceError
sqliteInternalError = sqlite3.InternalError
sqliteDataError = sqlite3.DataError
sqliteIntegrityError = sqlite3.IntegrityError
sqliteConnect = noop
sqliteOperationalError = NoopException
sqliteProgrammingError = NoopException
sqliteInterfaceError = NoopException
sqliteInternalError = NoopException
sqliteDataError = NoopException
sqliteIntegrityError = NoopException

def isSqlConnection(host, port, timeout: int = ..., product: Incomplete | None = ...): ...

class XPDBException(Exception):
    code: Incomplete
    message: Incomplete
    kwargs: Incomplete
    args: Incomplete
    def __init__(self, code, message, **kwargs) -> None: ...

class SqlDbConnection:
    modelXbrl: Incomplete
    disclosureSystem: Incomplete
    conn: Incomplete
    product: Incomplete
    syncSequences: bool
    tableColTypes: Incomplete
    tableColDeclaration: Incomplete
    accessionId: str
    tempInputTableName: Incomplete
    def __init__(self, modelXbrl, user, password, host, port, database, timeout, product, **kwargs) -> None: ...
    def close(self, rollback: bool = ...) -> None: ...
    @property
    def isClosed(self): ...
    def showStatus(self, msg, clearAfter: Incomplete | None = ...) -> None: ...
    def pyStrFromDbStr(self, str): ...
    def pyBoolFromDbBool(self, str): ...
    def pyNoneFromDbNULL(self, str) -> None: ...
    def dbNum(self, num): ...
    def dbStr(self, s): ...
    def dbTableName(self, tableName): ...
    @property
    def cursor(self): ...
    def closeCursor(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def dropTemporaryTable(self) -> None: ...
    def lockTables(self, tableNames, isSessionTransaction: bool = ...) -> None: ...
    def unlockAllTables(self) -> None: ...
    def execute(self, sql, commit: bool = ..., close: bool = ..., fetch: bool = ..., params: Incomplete | None = ..., action: str = ...): ...
    def create(self, ddlFiles, dropPriorTables: bool = ...): ...
    def databasesInDB(self): ...
    def dropAllTablesInDB(self) -> None: ...
    def tablesInDB(self): ...
    def sequencesInDB(self): ...
    def columnTypeFunctions(self, table): ...
    def getTable(self, table, idCol, newCols: Incomplete | None = ..., matchCols: Incomplete | None = ..., data: Incomplete | None = ..., commit: bool = ..., comparisonOperator: str = ..., checkIfExisting: bool = ..., insertIfNotMatched: bool = ..., returnMatches: bool = ..., returnExistenceStatus: bool = ...): ...
    def updateTable(self, table, cols: Incomplete | None = ..., data: Incomplete | None = ..., commit: bool = ...): ...
