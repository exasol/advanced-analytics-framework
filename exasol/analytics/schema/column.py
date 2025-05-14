from dataclasses import dataclass
from typing import Any

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_type import ColumnType
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclass(frozen=True, repr=True, eq=True)
class Column:
    name: ColumnName
    type: ColumnType

    def __post_init__(self):
        check_dataclass_types(self)

    @property
    def for_create(self) -> str:
        return self.sql_spec(for_create=True)

    def sql_spec(self, for_create: bool = False) -> str:
        return f"{self.name.fully_qualified} {self.type.sql_spec(for_create)}"

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
