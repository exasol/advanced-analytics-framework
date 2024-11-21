from typing import Optional, Union

from exasol.analytics.schema.schema_name import SchemaName
from exasol.analytics.schema.table_name import TableName
from exasol.analytics.schema.table_name_impl import TableNameImpl
from typeguard import TypeCheckError


class TableNameBuilder:

    def __init__(
        self,
        name: Optional[str] = None,
        schema: Optional[SchemaName] = None,
        table_name: Optional[TableName] = None,
    ):
        """
        Creates a builder for TableName objects,
        either by copying a TableName object (parameter "table_like_name") or
        using the newly provided values.
        Note that parameters schema/name have priority over parameter "table_like_name".
        """
        self._name = None
        self._schema_name = None
        if table_name is not None:
            self._name = table_name.name
            self._schema_name = table_name.schema_name
        if name is not None:
            self._name = name
        if schema is not None:
            self._schema_name = schema

    def with_name(self, name: str) -> "TableNameBuilder":
        self._name = name
        return self

    def with_schema_name(self, schema_name: SchemaName) -> "TableNameBuilder":
        self._schema_name = schema_name
        return self

    def build(self) -> TableName:
        if self._name is None:
            raise TypeCheckError("name must not be None")
        return self.create(self._name, self._schema_name)

    @staticmethod
    def create(name: str, schema: Optional[SchemaName] = None) -> TableName:
        return TableNameImpl(name, schema)
