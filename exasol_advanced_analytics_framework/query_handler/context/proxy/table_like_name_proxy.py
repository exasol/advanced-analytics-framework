from typing import Generic, TypeVar

from exasol_data_science_utils_python.schema.table_like_name import TableLikeName

from exasol_advanced_analytics_framework.query_handler.context.proxy.db_object_name_with_schema_proxy import \
    DBObjectNameWithSchemaProxy

NameType = TypeVar('NameType', bound=TableLikeName)


class TableLikeNameProxy(DBObjectNameWithSchemaProxy[NameType], TableLikeName, Generic[NameType]):

    def __init__(self, table_like_name: NameType, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)
