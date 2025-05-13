from typing import (
    Any,
    List,
    Tuple,
)

import pyexasol
from pyexasol import ExaStatement

from exasol.analytics.schema import Column
from exasol.analytics.sql_executor.interface import (
    ResultSet,
    SQLExecutor,
)

DEFAULT_FETCHMANY_SIZE = 10000


class PyExasolResultSet(ResultSet):
    def __init__(self, statement: ExaStatement):
        self.statement = statement

    def __iter__(self):
        return self

    def __next__(self) -> Tuple[Any]:
        return self.statement.__next__()

    def fetchone(self) -> Tuple[Any]:
        return self.statement.fetchone()

    def fetchmany(self, size=DEFAULT_FETCHMANY_SIZE) -> List[Tuple[Any]]:
        return self.statement.fetchmany(size)

    def fetchall(self) -> List[Tuple[Any]]:
        return self.statement.fetchall()

    def rowcount(self):
        return self.statement.rowcount()

    def columns(self) -> List[Column]:
        columns = [
            Column.from_pyexasol(column_name, column_type)
            for column_name, column_type in self.statement.columns().items()
        ]
        return columns

    def close(self):
        return self.statement.close()


class PyexasolSQLExecutor(SQLExecutor):

    def __init__(self, connection: pyexasol.ExaConnection):
        self._connection = connection

    def execute(self, sql: str) -> ResultSet:
        return PyExasolResultSet(self._connection.execute(sql))
