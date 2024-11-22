from typing import Optional

from typeguard import TypeCheckError

from exasol.analytics.schema.schema_name import SchemaName
from exasol.analytics.schema.view_name import ViewName
from exasol.analytics.schema.view_name_impl import ViewNameImpl


class ViewNameBuilder:

    def __init__(
        self,
        name: Optional[str] = None,
        schema: Optional[SchemaName] = None,
        view_name: Optional[ViewName] = None,
    ):
        """
        Creates a builder for ViewName objects,
        either by copying a ViewName (parameter view_name) object or
        using the newly provided values.
        Note that parameters schema/name have priority over parameter view_name.
        """
        self._name = None
        self._schema_name = None
        if view_name is not None:
            self._name = view_name.name
            self._schema_name = view_name.schema_name
        if name is not None:
            self._name = name
        if schema is not None:
            self._schema_name = schema

    def with_name(self, name: str) -> "ViewNameBuilder":
        self._name = name
        return self

    def with_schema_name(self, schema_name: SchemaName) -> "ViewNameBuilder":
        self._schema_name = schema_name
        return self

    def build(self) -> ViewName:
        if self._name is None:
            raise TypeCheckError("name must not be None")
        return self.create(self._name, self._schema_name)

    @staticmethod
    def create(name: str, schema: Optional[SchemaName] = None) -> ViewName:
        return ViewNameImpl(name, schema)
