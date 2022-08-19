from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropTableQuery
from exasol_advanced_analytics_framework.query_handler.context.proxy.table_like_proxy import TableLikeProxy
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class TableProxy(TableLikeProxy):
    def get_cleanup_query(self) -> Query:
        return DropTableQuery(self._table_name)
