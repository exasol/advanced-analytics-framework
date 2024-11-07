from exasol.analytics.schema import UDFName


from exasol.analytics.query_handler.context.proxy.db_object_name_with_schema_proxy import \
    DBObjectNameWithSchemaProxy
from exasol.analytics.query_handler.context.proxy.drop_udf_query import DropUDFQuery
from exasol.analytics.query_handler.query.query import Query


class UDFNameProxy(DBObjectNameWithSchemaProxy[UDFName], UDFName):

    def get_cleanup_query(self) -> Query:
        return DropUDFQuery(self._db_object_name)

    def __init__(self, script_name: UDFName, global_counter_value: int):
        super().__init__(script_name, global_counter_value)
