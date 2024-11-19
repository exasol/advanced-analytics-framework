from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema import DBObjectNameWithSchemaImpl, SchemaName, UDFName


class UDFNameImpl(DBObjectNameWithSchemaImpl, UDFName):

    @typechecked
    def __init__(self, udf_name: str, schema: Optional[SchemaName] = None):
        super().__init__(udf_name, schema)
