import dataclasses
from typing import Optional

import typeguard

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import ColumnType
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True, repr=True, eq=True)
class Column:
    name: ColumnName
    type: ColumnType

    @property
    def for_create(self) -> str:
        return f"{self.name.fully_qualified} {self.type.rendered}"

    def __post_init__(self):
        check_dataclass_types(self)


def decimal_column(
    name: str,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Column:
    type = ColumnType("DECIMAL", precision=precision, scale=scale)
    return Column(ColumnName(name), type)


def timestamp_column(name: str) -> Column:
    return Column(ColumnName(name), ColumnType("TIMESTAMP"))


def varchar_column(name: str, size: int) -> Column:
    return Column(ColumnName(name), ColumnType("VARCHAR", size=size))
