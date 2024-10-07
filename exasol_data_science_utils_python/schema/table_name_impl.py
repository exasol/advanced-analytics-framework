from typing import Optional

from typeguard import typechecked

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_like_name_impl import TableLikeNameImpl
from exasol_data_science_utils_python.schema.table_name import TableName


class TableNameImpl(TableLikeNameImpl, TableName):

    @typechecked
    def __init__(self, table_name: str, schema: Optional[SchemaName] = None):
        super().__init__(table_name, schema)
