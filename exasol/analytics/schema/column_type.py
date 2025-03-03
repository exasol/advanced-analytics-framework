import dataclasses
from typing import Any, Iterator, Optional

import typeguard

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


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

    @property
    def rendered(self) -> str:
        name = self.name.upper()
        def args() -> Iterator[Any]:
            if name == "VARCHAR":
                yield self.size
            elif name == "DECIMAL":
                yield self.precision
                if self.precision and self.scale:
                    yield self.scale

        suffix = ",".join(str(a) for a in args() if a)
        return f'{name}({suffix})' if suffix else name

    def __post_init__(self):
        check_dataclass_types(self)
