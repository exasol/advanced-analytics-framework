from typing import (
    Any,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas as pd

from exasol.analytics.query_handler.query.result.interface import (
    QueryResult,
    Row,
)
from exasol.analytics.schema import Column


class PythonQueryResult(QueryResult):
    def __getattr__(self, name: str) -> Any:
        return self[name]

    def __getitem__(self, item: Any) -> Any:
        index = self._column_name_index_mapping[item]
        return self._current_row[index]

    def next(self) -> bool:
        self._next()
        return self._current_row is not None

    def __iter__(self) -> Iterator[Row]:
        return self

    def __next__(self) -> Row:
        row = self._current_row
        if row is not None:
            self._next()
            return row
        else:
            raise StopIteration()

    def rowcount(self) -> int:
        return len(self._data)

    def columns(self) -> List[Column]:
        return list(self._columns)

    def column_names(self) -> List[str]:
        return [column.name.name for column in self._columns]

    def __init__(self, data: List[Tuple[Any, ...]], columns: List[Column]):
        self._columns = columns
        self._data = data
        self._iter = iter(data)
        self._column_name_index_mapping = {
            column.name.name: index for index, column in enumerate(columns)
        }
        self._next()

    def _range(self, num_rows: Union[int, str]) -> range:
        if isinstance(num_rows, int):
            return range(num_rows - 1)
        if num_rows == "all":
            return range(len(self._data) - 1)
        raise ValueError(f'num_rows must be an int or str "all" but is {num_rows}')

    def fetch_as_dataframe(
        self, num_rows: Union[int, str], start_col=0
    ) -> Optional[pd.DataFrame]:
        batch_list = []
        if self._current_row is not None:
            batch_list.append(self._current_row)
        for i in self._range(num_rows):
            self._next()
            if self._current_row is not None:
                batch_list.append(self._current_row)
            else:
                break
        self._next()
        if len(batch_list) > 0:
            df = pd.DataFrame(
                data=batch_list, columns=[column.name.name for column in self._columns]
            )  # TODO dtype
            df = df.iloc[:, start_col:]
            return df
        else:
            return None

    def _next(self):
        try:
            self._current_row = next(self._iter)
        except StopIteration:
            self._current_row = None
