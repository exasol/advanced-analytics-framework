from enum import (
    Enum,
    auto,
)


class DbObjectType(Enum):
    SCHEMA = auto()
    TABLE = auto()
    VIEW = auto()
    FUNCTION = auto()
    SCRIPT = auto()
