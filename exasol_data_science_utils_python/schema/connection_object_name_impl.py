from typing import cast

from exasol_data_science_utils_python.schema.connection_object_name import ConnectionObjectName
from exasol_data_science_utils_python.schema.dbobject_name_impl import DBObjectNameImpl
from exasol_data_science_utils_python.utils.repr_generation_for_object import generate_repr_for_object


class ConnectionObjectNameImpl(DBObjectNameImpl, ConnectionObjectName):

    def __init__(self, name: str):
        super().__init__(name)

    @property
    def normalized_name_for_udfs(self) -> str:
        return self.name.upper()

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name

    def __repr__(self):
        return generate_repr_for_object(self)

    def __eq__(self, other):
        # Connection names are case-insensitive https://docs.exasol.com/db/latest/sql/create_connection.htm
        return type(other) == type(self) and \
               self._name.upper() == cast(ConnectionObjectName, other).name.upper()

    def __hash__(self):
        # Connection names are case-insensitive https://docs.exasol.com/db/latest/sql/create_connection.htm
        assert len(self.__dict__) == 1, f"The attributes of {self.__class__} changed, " \
                                        f"you need to update the __hash__ method"
        return hash(self._name.upper())
