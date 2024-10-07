from typing import Union, Optional

from exasol_data_science_utils_python.schema.connection_object_name import ConnectionObjectName
from exasol_data_science_utils_python.schema.connection_object_name_impl import ConnectionObjectNameImpl
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl
from exasol_data_science_utils_python.schema.view_name import ViewName
from exasol_data_science_utils_python.schema.view_name_impl import ViewNameImpl


class ConnectionObjectNameBuilder:

    def __init__(self, name: str):
        self._name = name

    def build(self) -> ConnectionObjectName:
        return self.create(self._name)

    @classmethod
    def create(cls, name: str):
        return ConnectionObjectNameImpl(name)
