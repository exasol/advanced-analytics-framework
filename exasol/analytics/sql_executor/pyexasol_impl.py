from typing import Any, List, Tuple

import pyexasol  # type: ignore[import-untyped]
from pyexasol import ExaStatement

from exasol.analytics.schema import Column, ColumnName, ColumnNameBuilder, ColumnType
from exasol.analytics.sql_executor.interface import ResultSet, SQLExecutor

SRID = "srid"

FRACTION = "fraction"

WITH_LOCAL_TIME_ZONE = "withLocalTimeZone"

CHARACTER_SET = "characterSet"

SIZE = "size"

SCALE = "scale"

PRECISION = "precision"

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
            Column(
                ColumnNameBuilder.create(column_name),
                ColumnType(
                    name=column_type["type"],
                    precision=(
                        column_type[PRECISION] if PRECISION in column_type else None
                    ),
                    scale=column_type[SCALE] if SCALE in column_type else None,
                    size=column_type[SIZE] if SIZE in column_type else None,
                    characterSet=(
                        column_type[CHARACTER_SET]
                        if CHARACTER_SET in column_type
                        else None
                    ),
                    withLocalTimeZone=(
                        column_type[WITH_LOCAL_TIME_ZONE]
                        if WITH_LOCAL_TIME_ZONE in column_type
                        else None
                    ),
                    fraction=column_type[FRACTION] if FRACTION in column_type else None,
                    srid=column_type[SRID] if SRID in column_type else None,
                ),
            )
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
