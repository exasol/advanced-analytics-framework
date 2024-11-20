from typing import List, Union

from exasol.analytics.schema.column import Column
from exasol.analytics.schema.table import Table
from exasol.analytics.schema.table_name import TableName


class TableBuilder:
    def __init__(self, table: Union[Table, None] = None):
        self._name, self._columns = (
            (table.name, table.columns) if table
            else (None, [])
        )

    def with_name(self, name: TableName) -> "TableBuilder":
        self._name = name
        return self

    def with_columns(self, columns: List[Column]) -> "TableBuilder":
        self._columns = columns
        return self

    def build(self) -> Table:
        if self._name is None:
            raise ValueError("name must not be None")
        if not self._columns:
            raise ValueError("there must be at least one column")
        table = Table(self._name, self._columns)
        return table
