from typing import Optional

from typeguard import typechecked

from exasol_data_science_utils_python.schema.dbobject_name_with_schema_impl import DBObjectNameWithSchemaImpl
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.udf_name import UDFName


class UDFNameImpl(DBObjectNameWithSchemaImpl, UDFName):

    @typechecked
    def __init__(self, udf_name: str, schema: Optional[SchemaName] = None):
        super().__init__(udf_name, schema)
