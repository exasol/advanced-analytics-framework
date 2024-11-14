from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema import (
    SchemaName,
    TableLikeNameImpl,
    TableName,
)


class TableNameImpl(TableLikeNameImpl, TableName):

    @typechecked
    def __init__(self, table_name: str, schema: Optional[SchemaName] = None):
        super().__init__(table_name, schema)
