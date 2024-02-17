from exasol_data_science_utils_python.schema.dbobject_name import DBObjectName
from exasol_data_science_utils_python.schema.dbobject_name_impl import DBObjectNameImpl
from typeguard import typechecked


class ConnectionName(DBObjectName):
    """A DBObjectName class which represents the name of a connection object"""

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name.upper())


class ConnectionNameImpl(DBObjectNameImpl, ConnectionName):

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name)
