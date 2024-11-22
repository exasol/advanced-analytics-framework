from typing import Optional

from typeguard import TypeCheckError

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.table_like_name import TableLikeName


class ColumnNameBuilder:
    def __init__(
        self,
        name: Optional[str] = None,
        table_like_name: Optional[TableLikeName] = None,
        column_name: Optional[ColumnName] = None,
    ):
        """
        Creates a builder for ColumnName objects,
        either by copying a ColumnName object or
        using the newly provided values.
        """
        self._name = None
        self._table_like_name = None
        if column_name is not None:
            self._name = column_name.name
            self._table_like_name = column_name.table_like_name
        if name is not None:
            self._name = name
        if table_like_name is not None:
            self._table_like_name = table_like_name

    def with_name(self, name: str) -> "ColumnNameBuilder":
        self._name = name
        return self

    def with_table_like_name(
        self, table_like_name: TableLikeName
    ) -> "ColumnNameBuilder":
        self._table_like_name = table_like_name
        return self

    def build(self) -> ColumnName:
        if self._name is None:
            raise TypeCheckError("Name must not be None.")
        name = self.create(self._name, table_like_name=self._table_like_name)
        return name

    @staticmethod
    def create(
        name: str, table_like_name: Optional[TableLikeName] = None
    ) -> ColumnName:
        return ColumnName(name, table_like_name)
