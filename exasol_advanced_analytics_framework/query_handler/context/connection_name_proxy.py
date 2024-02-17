from exasol_data_science_utils_python.schema.dbobject_name import DBObjectName
from exasol_data_science_utils_python.schema.dbobject_name_impl import DBObjectNameImpl
from typeguard import typechecked

from exasol_advanced_analytics_framework.query_handler.context.proxy.db_object_name_proxy import DBObjectNameProxy
from exasol_advanced_analytics_framework.query_handler.query.drop_connection_query import DropConnectionQuery
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class ConnectionName(DBObjectName):
    """A DBObjectName class which represents the name of a connection object"""

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name.upper())


class ConnectionNameImpl(DBObjectNameImpl, ConnectionName):

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name)


class ConnectionNameProxy(DBObjectNameProxy[ConnectionName], ConnectionName):

    def get_cleanup_query(self) -> Query:
        return DropConnectionQuery(self._db_object_name)

    def __init__(self, connection_name: ConnectionName, global_counter_value: int):
        super().__init__(connection_name, global_counter_value)
