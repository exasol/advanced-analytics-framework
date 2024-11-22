import itertools
from typing import List, Optional, Tuple

from exasol.analytics.schema import Column
from exasol.analytics.sql_executor.interface import ResultSet


class MockResultSet(ResultSet):

    def __init__(
        self, rows: Optional[List[Tuple]] = None, columns: Optional[List[Column]] = None
    ):
        self._columns = columns
        self._rows = rows
        if rows is not None:
            if self._columns is not None:
                for row in rows:
                    if len(row) != len(self._columns):
                        raise AssertionError(
                            f"Row {row} doesn't fit columns {self._columns}"
                        )
            self._iter = self._rows.__iter__()

    def __iter__(self):
        if self._rows is None:
            raise NotImplementedError()
        else:
            return self

    def __next__(self) -> Tuple:
        if self._rows is None:
            raise NotImplementedError()
        else:
            return next(self._iter)

    def fetchone(self) -> Tuple:
        if self._rows is None:
            raise NotImplementedError()
        else:
            row = next(self)
            return row

    def fetchmany(self, size=1000) -> List[Tuple]:
        if self._rows is None:
            raise NotImplementedError()
        else:
            return [row for row in itertools.islice(self, size)]

    def fetchall(self) -> List[Tuple]:
        if self._rows is None:
            raise NotImplementedError()
        else:
            return [row for row in self]

    def rowcount(self):
        if self._rows is None:
            raise NotImplementedError()
        else:
            return len(self._rows)

    def columns(self) -> List[Column]:
        if self._columns is None:
            raise NotImplementedError()
        else:
            return self._columns

    def close(self):
        if self._rows is None:
            raise NotImplementedError()
