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
    scale: Optional[int] = None
    size: Optional[int] = None
    characterSet: Optional[str] = None
    withLocalTimeZone: Optional[bool] = None
    fraction: Optional[int] = None
    srid: Optional[int] = None
    unit: Optional[SizeUnit] = None

    @property
    def rendered(self) -> str:
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

        return "".join(elements())

    def __post_init__(self):
        check_dataclass_types(self)
