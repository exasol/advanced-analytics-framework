from typeguard import typechecked

from exasol.analytics.schema import (
    ExasolIdentifier,
    ExasolIdentifierImpl,
    TableLikeName,
)
from exasol.analytics.utils.hash_generation_for_object import generate_hash_for_object
from exasol.analytics.utils.repr_generation_for_object import generate_repr_for_object


class ColumnName(ExasolIdentifierImpl):
    @typechecked
    def __init__(self, name: str, table_like_name: TableLikeName | None = None):
        super().__init__(name)
        self._table_like_name = table_like_name

    @property
    def table_like_name(self):
        return self._table_like_name

    @property
    def fully_qualified(self) -> str:
        if self.table_like_name is not None:
            return f"{self._table_like_name.fully_qualified}.{self.quoted_name}"
        else:
            return self.quoted_name

    def __eq__(self, other):
        return (
            isinstance(other, ColumnName)
            and self._name == other.name
            and self._table_like_name == other.table_like_name
        )

    def __repr__(self):
        return generate_repr_for_object(self)

    def __hash__(self):
        return generate_hash_for_object(self)
