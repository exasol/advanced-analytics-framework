from exasol.analytics.query_handler.context.proxy.table_like_name_proxy import (
    TableLikeNameProxy,
)
from exasol.analytics.query_handler.query.drop.view import DropViewQuery
from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import ViewName


class ViewNameProxy(TableLikeNameProxy[ViewName], ViewName):

    def __init__(self, table_like_name: ViewName, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)

    def get_cleanup_query(self) -> Query:
        return DropViewQuery(self._db_object_name)
