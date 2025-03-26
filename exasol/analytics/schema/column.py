import dataclasses
from typing import Optional

import typeguard

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import (
    ColumnType,
    SizeUnit,
)
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
) -> Column:
    type = ColumnType("DECIMAL", precision=precision, scale=scale)
    return Column(ColumnName(name), type)


def timestamp_column(
    name: str,
    precision: Optional[int] = None,
) -> Column:
    return Column(ColumnName(name), ColumnType("TIMESTAMP", precision=precision))


def varchar_column(
    name: str,
    size: int,
    characterSet: str = "UTF8",
) -> Column:
    return Column(
        ColumnName(name),
        ColumnType(
            "VARCHAR",
            size=size,
            characterSet=characterSet,
        ),
    )


def hashtype_column(
    name: str,
    bytes: Optional[int] = None,
    bits: Optional[int] = None,
) -> Column:

    if bytes is not None and bits is not None:
        raise ValueError(
            "bytes and bits are specified at the same time:"
            f" bytes={bytes}, bits={bits}."
        )
    if bits:
        if bits % 8:
            raise ValueError(f"bits is not a multiple of 8: bits={bits}.")
        size, unit = bits, SizeUnit.BIT
    else:
        size, unit = bytes or 16, SizeUnit.BYTE

    return Column(ColumnName(name), ColumnType("HASHTYPE", size=size, unit=unit))
