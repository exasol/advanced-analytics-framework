import itertools
from typing import Tuple, Any, List, Dict, Optional

from exasol_data_science_utils_python.udf_utils.sql_executor import ResultSet


class MockResultSet(ResultSet):

    def __init__(self,
                 rows: Optional[List[Tuple]] = None,
                 columns: Optional[Dict[str, Dict[str, Any]]] = None
                 ):
        self._columns = columns
        self._rows = rows
        if rows is not None:
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

    def columns(self) -> Dict[str, Dict[str, Any]]:
        if self._columns is None:
            raise NotImplementedError()
        else:
            return self._columns

    def column_names(self) -> List[str]:
        if self._columns is None:
            raise NotImplementedError()
        else:
            return [column_name for column_name in self._columns.keys()]

    def close(self):
        if self._rows is None:
            raise NotImplementedError()
