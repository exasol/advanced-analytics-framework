from typing import Optional, Union

from exasol.analytics.schema.connection_object_name import ConnectionObjectName
from exasol.analytics.schema.connection_object_name_impl import ConnectionObjectNameImpl
from exasol.analytics.schema.schema_name import SchemaName
from exasol.analytics.schema.table_name import TableName
from exasol.analytics.schema.table_name_impl import TableNameImpl
from exasol.analytics.schema.view_name import ViewName
from exasol.analytics.schema.view_name_impl import ViewNameImpl


class ConnectionObjectNameBuilder:

    def __init__(self, name: str):
        self._name = name

    def build(self) -> ConnectionObjectName:
        return self.create(self._name)

    @classmethod
    def create(cls, name: str):
        return ConnectionObjectNameImpl(name)
