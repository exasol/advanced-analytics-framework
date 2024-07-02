from typing import Optional

from typeguard import typechecked

from exasol_data_science_utils_python.schema.dbobject_name_with_schema_impl import DBObjectNameWithSchemaImpl
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_like_name import TableLikeName


class TableLikeNameImpl(DBObjectNameWithSchemaImpl, TableLikeName):

    @typechecked
    def __init__(self, table_like_name: str, schema: Optional[SchemaName] = None):
        super().__init__(table_like_name, schema)
