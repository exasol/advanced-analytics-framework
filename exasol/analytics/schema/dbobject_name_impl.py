from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema.dbobject_name import DBObjectName
from exasol.analytics.schema.exasol_identifier_impl import ExasolIdentifierImpl
from exasol.analytics.utils.hash_generation_for_object import generate_hash_for_object
from exasol.analytics.utils.repr_generation_for_object import generate_repr_for_object


class DBObjectNameImpl(ExasolIdentifierImpl, DBObjectName):

    @typechecked
    def __init__(self, db_object_name: Optional[str]):
        super().__init__(db_object_name)

    def __repr__(self) -> str:
        return generate_repr_for_object(self)

    def __eq__(self, other) -> bool:
        return type(other) == type(self) and self.name == other.name

    def __hash__(self):
        return generate_hash_for_object(self)
