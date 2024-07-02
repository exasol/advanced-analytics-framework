from typing import Optional

from typeguard import typechecked

from exasol_data_science_utils_python.schema.dbobject_name_impl import DBObjectNameImpl
from exasol_data_science_utils_python.schema.dbobject_name_with_schema import DBObjectNameWithSchema
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.utils.hash_generation_for_object import generate_hash_for_object
from exasol_data_science_utils_python.utils.repr_generation_for_object import generate_repr_for_object


class DBObjectNameWithSchemaImpl(DBObjectNameImpl, DBObjectNameWithSchema):

    @typechecked
    def __init__(self, db_object_name: str, schema: Optional[SchemaName] = None):
        super().__init__(db_object_name)
        self._schema_name = schema

    @property
    def schema_name(self) -> SchemaName:
        return self._schema_name

    @property
    def fully_qualified(self) -> str:
        if self.schema_name is not None:
            return f'{self._schema_name.fully_qualified}.{self.quoted_name}'
        else:
            return self.quoted_name

    def __repr__(self) -> str:
        return generate_repr_for_object(self)

    def __eq__(self, other) -> bool:
        return type(other) == type(self) and \
               self._name == other.name and \
               self._schema_name == other.schema_name

    def __hash__(self):
        return generate_hash_for_object(self)
