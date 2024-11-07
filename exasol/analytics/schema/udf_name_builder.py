from typing import Optional

from exasol.analytics.schema import (
    UDFNameImpl,
    SchemaName,
    UDFName,
)


class UDFNameBuilder:

    def __init__(self,
                 name: Optional[str] = None,
                 schema: Optional[SchemaName] = None,
                 udf_name: Optional[UDFName] = None):
        """
        Creates a builder for UDFName objects,
        either by copying a UDFName object (parameter "udf_name") or
        using the newly provided values.
        Note that parameters schema/name have priority over parameter udf_name.
        """
        self._name = None
        self._schema_name = None
        if udf_name is not None:
            self._name = udf_name.name
            self._schema_name = udf_name.schema_name
        if name is not None:
            self._name = name
        if schema is not None:
            self._schema_name = schema

    def with_name(self, name: str) -> "UDFNameBuilder":
        self._name = name
        return self

    def with_schema_name(self, schema_name: SchemaName) -> "UDFNameBuilder":
        self._schema_name = schema_name
        return self

    def build(self) -> UDFName:
        return self.create(self._name, self._schema_name)

    @staticmethod
    def create(name: str, schema: Optional[SchemaName] = None) -> UDFName:
        return UDFNameImpl(name, schema)
