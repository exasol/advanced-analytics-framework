from typing import TypeVar, Generic

from exasol.analytics.schema import (
    DBObjectNameWithSchema,
    SchemaName,
)

from exasol.analytics.query_handler.context.proxy.db_object_name_proxy import DBObjectNameProxy

NameType = TypeVar('NameType', bound=DBObjectNameWithSchema)


class DBObjectNameWithSchemaProxy(DBObjectNameProxy[NameType], DBObjectNameWithSchema, Generic[NameType]):
    def __init__(self, db_object_name_with_schema: NameType, global_counter_value: int):
        super().__init__(db_object_name_with_schema, global_counter_value)

    @property
    def schema_name(self) -> SchemaName:
        self._check_if_released()
        return self._db_object_name.schema_name
