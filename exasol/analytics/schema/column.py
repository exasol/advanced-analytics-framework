from dataclasses import dataclass
from typing import Any

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import (
    BooleanType,
    CharType,
    ColumnType,
    DateType,
    DecimalType,
    DoublePrecisionType,
    GeometryType,
    HashSizeUnit,
    HashTypeType,
    TimeStampType,
    VarCharType,
)
from exasol.analytics.schema.column_type_utils import CharSet
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclass(frozen=True, repr=True, eq=True)
class Column:
    name: ColumnName
    type: ColumnType

    def __post_init__(self):
        check_dataclass_types(self)

    @property
    def for_create(self) -> str:
        return self.rendered

    @property
    def rendered(self) -> str:
        return f"{self.name.fully_qualified} {self.type.rendered}"

    @classmethod
    def from_sql_spec(cls, name: str, type_spec: str) -> "Column":
        return cls(
            name=ColumnName(name),
            type=ColumnType.from_sql_spec(type_spec),
        )

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "Column":
        return Column(
            name=ColumnName(column_name),
            type=ColumnType.from_pyexasol(pyexasol_args),
        )


def boolean_column(name: str) -> Column:
    """
    Instanciate a Column with this class as its type and its name
    specified as a simple str, rather than a ColumnName object.
    """
    return Column(ColumnName(name), BooleanType())


def char_column(
    name: str,
    size: int = 1,
    charset: CharSet = CharSet.UTF8,
) -> Column:
    return Column(ColumnName(name), CharType(size, charset))


def date_column(name: str) -> Column:
    return Column(ColumnName(name), DateType())


def decimal_column(name: str, precision: int = 18, scale: int = 0) -> Column:
    return Column(ColumnName(name), DecimalType(precision, scale))


def double_column(name: str) -> Column:
    return Column(ColumnName(name), DoublePrecisionType())


def geometry_column(name: str, srid: int = 0) -> Column:
    return Column(ColumnName(name), GeometryType(srid))


def hashtype_column(
    name: str,
    size: int = 16,
    unit: HashSizeUnit = HashSizeUnit.BYTE,
) -> Column:
    return Column(ColumnName(name), HashTypeType(size, unit))


def timestamp_column(
    name: str,
    precision: int = 3,
    local_time_zone: bool = False,
) -> Column:
    return Column(ColumnName(name), TimeStampType(precision, local_time_zone))


def varchar_column(
    name: str,
    size: int,
    charset: CharSet = CharSet.UTF8,
) -> Column:
    return Column(ColumnName(name), VarCharType(size, charset))
