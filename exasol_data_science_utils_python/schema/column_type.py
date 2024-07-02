import dataclasses
from typing import Optional

import typeguard

from exasol_data_science_utils_python.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True, repr=True, eq=True)
class ColumnType:
    name: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    size: Optional[int] = None
    characterSet: Optional[str] = None
    withLocalTimeZone: Optional[bool] = None
    fraction: Optional[int] = None
    srid: Optional[int] = None

    def __post_init__(self):
        check_dataclass_types(self)