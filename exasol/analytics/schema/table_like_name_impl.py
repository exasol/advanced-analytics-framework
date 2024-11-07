from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema import (
    DBObjectNameWithSchemaImpl,
    SchemaName,
    TableLikeName,
)


class TableLikeNameImpl(DBObjectNameWithSchemaImpl, TableLikeName):

    @typechecked
    def __init__(self, table_like_name: str, schema: Optional[SchemaName] = None):
        super().__init__(table_like_name, schema)
