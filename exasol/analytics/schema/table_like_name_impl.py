from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema.dbobject_name_with_schema_impl import DBObjectNameWithSchemaImpl
from exasol.analytics.schema.schema_name import SchemaName
from exasol.analytics.schema.table_like_name import TableLikeName


class TableLikeNameImpl(DBObjectNameWithSchemaImpl, TableLikeName):

    @typechecked
    def __init__(self, table_like_name: str, schema: Optional[SchemaName] = None):
        super().__init__(table_like_name, schema)
