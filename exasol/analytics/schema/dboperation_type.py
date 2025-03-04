from enum import (
    Enum,
    auto,
)


class DbOperationType(Enum):
    CREATE = auto()
    CREATE_OR_REPLACE = auto()
    CREATE_IF_NOT_EXISTS = auto()
    ALTER = auto()
    DROP = auto()
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    EXECUTE = auto()
