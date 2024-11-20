from typing import Union

from exasol.analytics.schema.column import Column
from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import ColumnType


class ColumnBuilder:
    def __init__(self, column: Union[Column, None] = None):
        self._name, self._type = (
            (None, None) if column is None
            else (column.name, column.type)
        )
        # if column is not None:
        #     self._name = column.name
        #     self._type = column.type
        # else:
        #     self._name = None
        #     self._type = None

    def with_name(self, name: ColumnName) -> "ColumnBuilder":
        self._name = name
        return self

    def with_type(self, type: ColumnType) -> "ColumnBuilder":
        self._type = type
        return self

    def build(self) -> Column:
        if self._name is None:
            raise ValueError("name must not be None")
        if self._type is None:
            raise ValueError("type must not be None")
        column = Column(self._name, self._type)
        return column
