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
    scale: Optional[int] = 0,
    comment: Optional[str] = None,
) -> Column:
    type = ColumnType("DECIMAL", precision=precision, scale=scale)
    return Column(ColumnName(name), type)


def timestamp_column(
    name: str,
    precision: Optional[int] = None,
    comment: Optional[str] = None,
) -> Column:
    return Column(ColumnName(name), ColumnType("TIMESTAMP", precision=precision))


def varchar_column(
    name: str,
    size: int,
    characterSet: str = "UTF8",
    comment: Optional[str] = None,
) -> Column:
    return Column(
        ColumnName(name),
        ColumnType(
            "VARCHAR",
            size=size,
            characterSet=characterSet,
        ),
    )
