from exasol_advanced_analytics_framework.query_handler.context.connection_name import ConnectionName, ConnectionNameImpl
from exasol_advanced_analytics_framework.query_handler.context.proxy.db_object_name_proxy import DBObjectNameProxy
from exasol_advanced_analytics_framework.query_handler.query.drop_connection_query import DropConnectionQuery
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class ConnectionNameProxy(DBObjectNameProxy[ConnectionName], ConnectionName):

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name

    def get_cleanup_query(self) -> Query:
        return DropConnectionQuery(self._db_object_name)

    def __init__(self, connection_name: ConnectionName, global_counter_value: int):
        super().__init__(connection_name, global_counter_value)
