from enum import Enum, auto


class DBObjectType(Enum):
    SCHEMA = auto()
    TABLE = auto()
    VIEW = auto()
    FUNCTION = auto()
    SCRIPT = auto()
