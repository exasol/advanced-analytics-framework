import re
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Optional,
)


class CharSet(Enum):
    UTF8 = "UTF8"
    ASCII = "ASCII"


class PyexasolOption(Enum):
    CHARACTER_SET = "characterSet"
    PRECISION = "precision"
    SCALE = "scale"
    SIZE = "size"
    SRID = "srid"
    UNIT = "unit"
    WITH_LOCAL_TIME_ZONE = "withLocalTimeZone"


@dataclass(frozen=True)
class PyexasolMapping:
    int_keys: list[PyexasolOption]
    modifier_key: Optional[PyexasolOption] = None
    modifier_getter: Callable[[dict[str, Any]], str] = lambda x: ""

    def modifier(self, args: dict[str, Any]) -> str:
        return (
            str(args.get(self.modifier_key.value, ""))
            if self.modifier_key
            else self.modifier_getter(args)
        )


class ColumnTypeSource(Enum):
    SQL = "SQL"
    PYEXASOL = "PYEXASOL"


@dataclass(frozen=True)
class SqlType:
    """
    Represents an SQL column type, examples:

    * SqlType("VARCHAR", [100], "ASCII")
    * SqlType("DECIMAL", [10,2], "")
    * SqlType("HASHTYPE", [8], "BIT")
    * SqlType("TIMESTAMP", [], "WITH LOCAL TIME ZONE")
    """

    name: str
    int_args: list[int]
    modifier: str
    source: ColumnTypeSource = ColumnTypeSource.SQL

    def int_dict(self, keys: list[str]) -> dict[str, Any]:
        return dict(zip(keys, self.int_args))

    @property
    def char_type_args(self) -> dict[str, Any]:
        args: dict[str, Any] = dict(zip(["size"], self.int_args))
        if self.modifier:
            args["charset"] = CharSet(self.modifier)
        return args

    @classmethod
    def from_string(cls, spec: str) -> "SqlType":
        """
        Instantiate SqlType based on its string representation, examples:

        * "VARCHAR(100)"
        * "DECIMAL(10,2)"
        * "HASHTYPE(8 BIT)"
        * "TIMESTAMP WITH LOCAL TIME ZONE"
        * "CHAR ASCII"
        """

        raw = spec.strip().upper()
        name = raw
        paren = ""
        modifier = ""
        if raw == "DOUBLE PRECISION":
            name = raw
        elif "(" in raw:
            name, paren, modifier = re.split(" *[()] *", raw)
        elif " " in raw:
            name, modifier = raw.split(" ", maxsplit=1)

        if name == "VARCHAR" and not paren:
            raise ValueError(f'Missing required argument size in "{spec}"')

        if name == "HASHTYPE" and " " in paren:
            paren, modifier = paren.split()

        int_args = [int(i) for i in paren.split(",")] if paren else []
        return SqlType(name, int_args, modifier, ColumnTypeSource.SQL)

    @classmethod
    def from_pyexasol(cls, args: dict[str, Any], mapping: PyexasolMapping) -> "SqlType":
        sql_name = args["type"]
        sql_keys = [k.value for k in mapping.int_keys]
        int_args = [args[k] for k in args if k in sql_keys]
        modifier = mapping.modifier(args)
        return SqlType(sql_name, int_args, modifier, ColumnTypeSource.PYEXASOL)
