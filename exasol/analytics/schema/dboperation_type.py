from enum import (
    Enum,
    auto,
)


class DbOperationType(Enum):
    ALTER = auto()
    CREATE = auto()
    CREATE_OR_REPLACE = auto()
    CREATE_IF_NOT_EXISTS = auto()
    DELETE = auto()
    DROP = auto()
    EXECUTE = auto()
    INSERT = auto()
    SELECT = auto()
    UPDATE = auto()
