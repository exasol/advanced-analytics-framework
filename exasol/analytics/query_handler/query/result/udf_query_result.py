import collections
from typing import Any, ForwardRef, Iterator, List, Optional, OrderedDict, Union

from exasol.analytics.query_handler.query.result.interface import QueryResult, Row
from exasol.analytics.schema.column import Column
from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import ColumnType

class UDFQueryResult(QueryResult):

    def __init__(
        self, ctx, exa, column_mapping: OrderedDict[str, str], start_col: int = 0
    ):
        self._start_col = start_col
        self._ctx = ctx
        self._has_next = True
        self._reverse_column_mapping = collections.OrderedDict(
            [(value, key) for key, value in column_mapping.items()]
        )
        self._columns = self._compute_columns(exa)
        self._initialized = False

    def __getattr__(self, name):
        if name in self._reverse_column_mapping:
            return self._ctx[self._reverse_column_mapping[name]]
        else:
            raise AttributeError(f"Attribute {name} not found.")

    def __getitem__(self, item: Any) -> Any:
        return self._ctx[self._reverse_column_mapping[item]]

    def next(self) -> bool:
        self._initialized = True
        return self._ctx.next()

    def __iter__(self) -> Iterator[Row]:
        return self

    def __next__(self) -> Row:
        if self._initialized:
            if not self._ctx.next():
                raise StopIteration()
        else:
            self._initialized = True
        row = tuple(self._ctx[value] for value in self._reverse_column_mapping.values())
        return row

    def rowcount(self) -> int:
        return self._ctx.size()

    def fetch_as_dataframe(
        self, num_rows: Union[str, int], start_col: int = 0
    ) -> Optional[ForwardRef("pandas.DataFrame")]:  # type: ignore[valid-type]
        # This place intentionally uses a forward reference to avoid importing
        # pandas which might take several seconds.
        df = self._ctx.get_dataframe(num_rows, start_col=self._start_col)
        self._initialized = True
        if df is None:
            return None
        else:
            filtered_df = df[self._reverse_column_mapping.values()]
            filtered_df.columns = list(self._reverse_column_mapping.keys())
            filtered_df_from_start_col = filtered_df.iloc[:, start_col:]
            return filtered_df_from_start_col

    def columns(self) -> List[Column]:
        return list(self._columns)

    def _compute_columns(self, exa) -> List[Column]:
        column_dict = {
            column.name: column.sql_type for column in exa.meta.input_columns
        }
        columns = [
            Column(ColumnName(key), ColumnType(column_dict[value]))
            for key, value in self._reverse_column_mapping.items()
        ]
        return columns

    def column_names(self) -> List[str]:
        return list(self._reverse_column_mapping.keys())
