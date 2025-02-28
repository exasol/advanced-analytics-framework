from enum import Enum, auto


class DBOperationType(Enum):
    CREATE = auto()
    ALTER = auto()
    DROP = auto()
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    EXECUTE = auto()
