from exasol.analytics.schema import TableName


from exasol.analytics.query_handler.context.proxy.table_like_name_proxy import TableLikeNameProxy
from exasol.analytics.query_handler.query.drop.table import DropTableQuery
from exasol.analytics.query_handler.query.interface import Query


class TableNameProxy(TableLikeNameProxy[TableName], TableName):

    def __init__(self, table_like_name: TableName, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)

    def get_cleanup_query(self) -> Query:
        return DropTableQuery(self._db_object_name)
