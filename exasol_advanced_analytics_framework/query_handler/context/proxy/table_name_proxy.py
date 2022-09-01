from exasol_data_science_utils_python.schema.table_name import TableName

from exasol_advanced_analytics_framework.query_handler.context.proxy.table_like_name_proxy import TableLikeNameProxy
from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropTableQuery
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class TableNameProxy(TableLikeNameProxy[TableName], TableName):

    def __init__(self, table_like_name: TableName, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)

    def get_cleanup_query(self) -> Query:
        return DropTableQuery(self._db_object_name)
