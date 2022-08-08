from exasol_advanced_analytics_framework.event_handler.query.drop_table_query import DropTableQuery
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_like_proxy import TableLikeProxy
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class TableProxy(TableLikeProxy):
    def get_cleanup_query(self) -> Query:
        return DropTableQuery(self._name)
