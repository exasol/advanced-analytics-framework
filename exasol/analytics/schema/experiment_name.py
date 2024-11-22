from typeguard import typechecked

from exasol.analytics.schema.exasol_identifier import ExasolIdentifier
from exasol.analytics.schema.exasol_identifier_impl import ExasolIdentifierImpl
from exasol.analytics.utils.hash_generation_for_object import generate_hash_for_object
from exasol.analytics.utils.repr_generation_for_object import generate_repr_for_object


class ExperimentName(ExasolIdentifierImpl):
    @typechecked
    def __init__(self, name: str):
        super().__init__(name)

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name

    def __eq__(self, other):
        return isinstance(other, ExperimentName) and self._name == other.name

    def __repr__(self):
        return generate_repr_for_object(self)

    def __hash__(self):
        return generate_hash_for_object(self)
