from typing import Union

from exasol.analytics.schema import Column, ColumnName, ColumnType


class ColumnBuilder:
    def __init__(self, column: Union[Column, None] = None):
        if column is not None:
            self._name = column.name
            self._type = column.type
        else:
            self._name = None
            self._type = None

    def with_name(self, name: ColumnName) -> "ColumnBuilder":
        self._name = name
        return self

    def with_type(self, type: ColumnType) -> "ColumnBuilder":
        self._type = type
        return self

    def build(self) -> Column:
        column = Column(self._name, self._type)
        return column
