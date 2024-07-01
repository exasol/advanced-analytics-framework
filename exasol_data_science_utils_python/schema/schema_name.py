from typeguard import typechecked

from exasol_data_science_utils_python.schema.dbobject_name_impl import DBObjectNameImpl


class SchemaName(DBObjectNameImpl):

    @typechecked
    def __init__(self, schema_name: str):
        super().__init__(schema_name)

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name
