import dataclasses

import typeguard

from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from exasol_data_science_utils_python.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True, repr=True, eq=True)
class Column:
    name: ColumnName
    type: ColumnType

    def __post_init__(self):
        check_dataclass_types(self)