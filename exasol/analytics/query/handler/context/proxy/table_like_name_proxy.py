from typing import Generic, TypeVar

from exasol.analytics.schema import TableLikeName


from exasol.analytics.query.handler.context.proxy.db_object_name_with_schema_proxy import     DBObjectNameWithSchemaProxy

NameType = TypeVar('NameType', bound=TableLikeName)


class TableLikeNameProxy(DBObjectNameWithSchemaProxy[NameType], TableLikeName, Generic[NameType]):

    def __init__(self, table_like_name: NameType, global_counter_value: int):
        super().__init__(table_like_name, global_counter_value)
