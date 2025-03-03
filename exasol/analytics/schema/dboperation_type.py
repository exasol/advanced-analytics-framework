from enum import (
    Enum,
    auto,
)


class DbOperationType(Enum):
    CREATE = auto()
    ALTER = auto()
    DROP = auto()
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    EXECUTE = auto()
