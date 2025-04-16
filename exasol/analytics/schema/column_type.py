import dataclasses
from enum import (
    Enum,
    auto,
)
from typing import (
    Any,
    Iterator,
    Optional,
)

import typeguard

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


class SizeUnit(Enum):
    BYTE = auto()
    BIT = auto()


@dataclasses.dataclass(frozen=True, repr=True, eq=True)
class ColumnType:
    name: str
    precision: Optional[int] = None
    """For column types DECIMAL and INTEGER."""
    scale: Optional[int] = None
    """For column types DECIMAL and INTEGER."""
    size: Optional[int] = None
    """For column types CHAR and VARCHAR."""
    characterSet: Optional[str] = None
    """For column types CHAR and VARCHAR. Supported values: "ASCII" and "UTF8"."""
    withLocalTimeZone: Optional[bool] = None
    """Only for column type TIMESTAMP."""
    fraction: Optional[int] = None
    """Number of fractional seconds for column type TIMESTAMP."""
    srid: Optional[int] = None
    """Spatial reference system identifier, only for column type GEOMETRY."""
    unit: Optional[SizeUnit] = None
    """Only for column type HASHTYPE. Supported values: "BYTE" and "BIT"."""

    @property
    def rendered(self) -> str:
        """
        Return a string representing the type including all parameters
        such as scale or precision appropriate for creating an SQL statement
        CREATE TABLE.
        """
        name = self.name.upper()

        def args() -> Iterator[Any]:
            if name == "TIMESTAMP":
                yield self.precision
            elif name == "VARCHAR":
                yield self.size
            elif name == "DECIMAL":
                yield self.precision
                if self.precision is not None and self.scale is not None:
                    yield self.scale
            elif name == "HASHTYPE":
                if self.size and self.unit:
                    yield f"{self.size} {self.unit.name}"
                else:
                    yield "16 BYTE"

        def elements() -> Iterator[str]:
            yield name
            infix = ",".join(str(a) for a in args() if a is not None)
            if infix:
                yield f"({infix})"
            if name == "VARCHAR":
                yield f' {self.characterSet or "UTF8"}'
            if (name == "TIMESTAMP") and self.withLocalTimeZone:
                yield " WITH LOCAL TIME ZONE"

        return "".join(elements())

    def __post_init__(self):
        check_dataclass_types(self)
