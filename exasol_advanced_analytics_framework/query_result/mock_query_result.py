from typing import List, Tuple, Any, Union, Optional, Iterator

import pandas as pd
from exasol_udf_mock_python.column import Column

from exasol_advanced_analytics_framework.query_result.query_result import QueryResult, Row


class MockQueryResult(QueryResult):
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
        self._column_name_index_mapping = {column.name.name: index for index, column in enumerate(columns)}
        self._next()

    def fetch_as_dataframe(self, num_rows: Union[int, str], start_col=0) -> Optional[pd.DataFrame]:
        batch_list = []
        if num_rows == "all":
            num_rows = len(self._data)
        if self._current_row is not None:
            batch_list.append(self._current_row)
        for i in range(num_rows - 1):
            self._next()
            if self._current_row is not None:
                batch_list.append(self._current_row)
            else:
                break
        self._next()
        if len(batch_list) > 0:
            df = pd.DataFrame(data=batch_list,
                              columns=[column.name.name for column in self._columns])  # TODO dtype
            df = df.iloc[:, start_col:]
            return df
        else:
            return None

    def _next(self):
        try:
            self._current_row = next(self._iter)
        except StopIteration:
            self._current_row = None
