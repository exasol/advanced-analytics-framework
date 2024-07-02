from abc import ABC
from typing import List, TypeVar, Generic

from typeguard import typechecked

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.dbobject import DBObject
from exasol_data_science_utils_python.schema.table_like_name import TableLikeName
from exasol_data_science_utils_python.utils.hash_generation_for_object import generate_hash_for_object
from exasol_data_science_utils_python.utils.repr_generation_for_object import generate_repr_for_object

NameType = TypeVar('NameType', bound=TableLikeName)


class TableLike(DBObject[NameType], ABC):

    @typechecked
    def __init__(self, name: NameType, columns: List[Column]):
        super().__init__(name)
        self._columns = columns
        if len(self._columns)==0:
            raise ValueError("At least one column needed.")
        unique_column_names = {column.name for column in self.columns}
        if len(unique_column_names) != len(columns):
            raise ValueError("Column names are not unique.")

    @property
    def columns(self) -> List[Column]:
        return list(self._columns)

    def __eq__(self, other):
        return super().__eq__(other) and \
               self._columns == other.columns

    def __hash__(self):
        return generate_hash_for_object(self)
