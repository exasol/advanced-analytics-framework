from exasol_advanced_analytics_framework.event_handler.query.drop_view_query import DropViewQuery
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_like_proxy import TableLikeProxy
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class ViewProxy(TableLikeProxy):
    def get_cleanup_query(self) -> Query:
        return DropViewQuery(self._table_name)
