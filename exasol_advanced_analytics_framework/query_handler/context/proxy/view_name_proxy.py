from exasol_data_science_utils_python.schema.view_name import ViewName

from exasol_advanced_analytics_framework.query_handler.context.proxy.table_like_name_proxy import TableLikeNameProxy
from exasol_advanced_analytics_framework.query_handler.query.drop_view_query import DropViewQuery
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class ViewNameProxy(TableLikeNameProxy[ViewName], ViewName):

    def __init__(self, table_like_name: ViewName, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)

    def get_cleanup_query(self) -> Query:
        return DropViewQuery(self._db_object_name)
