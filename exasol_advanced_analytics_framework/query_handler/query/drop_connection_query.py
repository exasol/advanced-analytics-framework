from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.view_name import ViewName

from exasol_advanced_analytics_framework.query_handler.context.connection_name_proxy import ConnectionName
from exasol_advanced_analytics_framework.query_handler.query.drop_query import DropQuery


class DropConnectionQuery(DropQuery):

    def __init__(self, connection_name: ConnectionName):
        self._connection_name = connection_name

    @property
    def query_string(self) -> str:
        return f"DROP CONNECTION IF EXISTS {self._connection_name.fully_qualified};"

    @property
    def connection_name(self) -> ConnectionName:
        return self._connection_name
