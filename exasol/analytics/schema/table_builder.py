from typing import (
    List,
    Optional,
    Union,
)

from typeguard import TypeCheckError

from exasol.analytics.schema.column import Column
from exasol.analytics.schema.table import Table
from exasol.analytics.schema.table_name import TableName


class TableBuilder:
    def __init__(self, table: Union[Table, None] = None):
        self._name: Optional[TableName]
        self._columns: list[Column] = []
        self._name, self._columns = (table.name, table.columns) if table else (None, [])

    def with_name(self, name: TableName) -> "TableBuilder":
        self._name = name
        return self

    def with_columns(self, columns: list[Column]) -> "TableBuilder":
        self._columns = columns
        return self

    def build(self) -> Table:
        if self._name is None:
            raise TypeCheckError("Name must not be None.")
        if not self._columns:
            raise TypeCheckError("There must be at least one column.")
        table = Table(self._name, self._columns)
        return table
