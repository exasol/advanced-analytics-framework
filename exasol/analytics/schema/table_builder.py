from typing import List, Union

from exasol.analytics.schema import Column, Table, TableName


class TableBuilder:
    def __init__(self, table: Union[Table, None] = None):
        if table is not None:
            self._name = table.name
            self._columns = table.columns
        else:
            self._name = None
            self._columns = None

    def with_name(self, name: TableName) -> "TableBuilder":
        self._name = name
        return self

    def with_columns(self, columns: List[Column]) -> "TableBuilder":
        self._columns = columns
        return self

    def build(self) -> Table:
        table = Table(self._name, self._columns)
        return table
