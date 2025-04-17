# do not from __future__ import annotations
# as this breaks typeguard checks

import re
from abc import abstractmethod
from dataclasses import dataclass
from enum import (
    Enum,
    auto,
)
from typing import (
    Any,
    Optional,
)

import typeguard

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclass(frozen=True)
class SqlType:
    type_name: str
    args: list[int]
    options: str

    @classmethod
    def from_string(cls, sql_type: str) -> "SqlType":
        raw = sql_type.strip().upper()
        if m := re.search(r"\((.*)\)", raw):
            type_name = raw[: m.start()].strip()
            paren = m.group(1)
            options = raw[m.end() :].strip()

            if type_name == "HASHTYPE":
                words = paren.split()
                args = [int(words[0])]
                options = words[1]
                return SqlType(type_name, args, options)
            else:
                args = [int(i) for i in m.group(1).split(",")]
                return SqlType(type_name, args, options)

        if raw == "DOUBLE PRECISION":
            return SqlType(raw, [], "")

        tokens = raw.split(maxsplit=1)
        return SqlType(tokens[0], [], tokens[1] if len(tokens) > 1 else "")


class PyexasolTypes:
    SRID = "srid"
    FRACTION = "fraction"
    WITH_LOCAL_TIME_ZONE = "withLocalTimeZone"
    CHARACTER_SET = "characterSet"
    SIZE = "size"
    SCALE = "scale"
    PRECISION = "precision"
    UNIT = "unit"


class classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


@dataclass(frozen=True, repr=True, eq=True)
class Column:
    name: ColumnName

    @property
    def for_create(self) -> str:
        return f"{self.name.fully_qualified} {self.rendered}"

    @property
    @abstractmethod
    def rendered(self) -> str: ...

    @classproperty
    def _sql_to_classname(cls) -> dict[str, Any]:
        classes = {
            getattr(c, "sql_name"): c
            for c in [
                BooleanColumn,
                CharColumn,
                DateColumn,
                DecimalColumn,
                DoublePrecisionColumn,
                GeometryColumn,
                HashTypeColumn,
                TimeStampColumn,
                VarCharColumn,
            ]
        }
        classes["INTEGER"] = DecimalColumn
        classes["FLOAT"] = DoublePrecisionColumn
        classes["DOUBLE"] = DoublePrecisionColumn
        return classes

    @classmethod
    def from_pyexasol_type(
        cls,
        column_name: str,
        sql_type: str,
        pyexasol_args: dict[str, Any] = {},
    ) -> "Column":
        classes = cls._sql_to_classname
        return classes[sql_type].from_pyexasol(column_name, pyexasol_args)

    @classmethod
    def from_sql_type(cls, column_name: str, sql_type: str) -> "Column":
        parsed = SqlType.from_string(sql_type)
        classes = cls._sql_to_classname
        return classes[parsed.type_name].from_sql(
            column_name,
            parsed.args,
            parsed.options,
        )

    def __post_init__(self):
        check_dataclass_types(self)


def pyexasol_type_args(
    sql_values: dict[str, Any],
    keys: list[str] | dict[str, Any] = {},
) -> dict[str, Any]:
    """
    Return a dict { ca: v } with each key `ca` being the name of an
    attribute of one of the subclasses of Column.

    :sql_values: dict of { s: v }, with each key `s` being the name of an SQL
                 type argument.

    :keys: is either a list of str or a mapping of sql_type argument names to
           the names of attributes of one of the subclasses of Column.
    """
    keys = keys if isinstance(keys, dict) else {k: k for k in keys}
    return {ca: sql_values[s] for s, ca in keys.items() if s in sql_values}


CHAR_TYPE_ARGS = {
    PyexasolTypes.SIZE: "size",
    PyexasolTypes.CHARACTER_SET: "charset",
}


@dataclass(frozen=True, repr=True, eq=True)
class BooleanColumn(Column):
    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "BOOLEAN"

    @property
    def rendered(self) -> str:
        return self.sql_name

    @classmethod
    def simple(cls, name: str) -> "BooleanColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "BooleanColumn":
        return cls.simple(column_name)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "BooleanColumn":
        return cls.from_pyexasol(column_name, {})


class CharSet(Enum):
    UTF8 = auto()
    ASCII = auto()

    @classmethod
    def from_string(cls, name: str) -> "CharSet":
        for c in cls:
            if c.name == name:
                return c
        raise ValueError(f"Couldn't find CharSet with name '{name}'")


@dataclass(frozen=True, repr=True, eq=True)
class CharColumn(Column):
    size: int = 1
    charset: CharSet = CharSet.UTF8

    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "CHAR"

    @property
    def rendered(self) -> str:
        return f"{self.sql_name}({self.size}) CHARACTER SET {self.charset.name}"

    @classmethod
    def simple(
        cls,
        name: str,
        size: int = 1,
        charset: CharSet = CharSet.UTF8,
    ) -> "CharColumn":
        return cls(ColumnName(name), size, charset)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "CharColumn":
        args = pyexasol_type_args(pyexasol_args, CHAR_TYPE_ARGS)
        if charset := args.get("charset"):
            args["charset"] = CharSet.from_string(charset)
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(cls, column_name: str, args: list[int], options: str) -> "CharColumn":
        pyexasol_args: dict[str, Any] = {PyexasolTypes.SIZE: args[0]} if args else {}
        if options:
            pyexasol_args[PyexasolTypes.CHARACTER_SET] = options
        return cls.from_pyexasol(column_name, pyexasol_args)


@dataclass(frozen=True, repr=True, eq=True)
class DateColumn(Column):
    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "DATE"

    @property
    def rendered(self) -> str:
        return self.sql_name

    @classmethod
    def simple(cls, name: str) -> "DateColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "DateColumn":
        return cls.simple(column_name)

    @classmethod
    def from_sql(cls, column_name: str, args: list[int], options: str) -> "DateColumn":
        return cls.from_pyexasol(column_name, {})


@dataclass(frozen=True, repr=True, eq=True)
class DecimalColumn(Column):
    precision: int = 18
    scale: int = 0

    @classproperty
    def sql_name(cls) -> str:
        return "DECIMAL"

    @property
    def rendered(self) -> str:
        return f"{self.sql_name}({self.precision},{self.scale})"

    def __post_init__(self):
        check_dataclass_types(self)

    @classmethod
    def simple(cls, name: str, precision: int = 18, scale: int = 0) -> "DecimalColumn":
        return cls(ColumnName(name), precision, scale)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "DecimalColumn":
        args = pyexasol_type_args(
            pyexasol_args,
            [PyexasolTypes.PRECISION, PyexasolTypes.SCALE],
        )
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "DecimalColumn":
        pyexasol_args = {}
        if len(args) > 0:
            pyexasol_args[PyexasolTypes.PRECISION] = args[0]
        if len(args) > 1:
            pyexasol_args[PyexasolTypes.SCALE] = args[1]
        return cls.from_pyexasol(column_name, pyexasol_args)


@dataclass(frozen=True, repr=True, eq=True)
class DoublePrecisionColumn(Column):
    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "DOUBLE PRECISION"

    @property
    def rendered(self) -> str:
        return self.sql_name

    @classmethod
    def simple(cls, name: str) -> "DoublePrecisionColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "DoublePrecisionColumn":
        return cls.simple(column_name)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "DoublePrecisionColumn":
        return cls.from_pyexasol(column_name, {})


@dataclass(frozen=True, repr=True, eq=True)
class GeometryColumn(Column):
    srid: int = 0
    "Spatial reference identifier"

    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "GEOMETRY"

    @property
    def rendered(self) -> str:
        return f"{self.sql_name}({self.srid})"

    @classmethod
    def simple(cls, name: str, srid: int = 0) -> "GeometryColumn":
        return cls(ColumnName(name), srid)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "GeometryColumn":
        args = pyexasol_type_args(pyexasol_args, [PyexasolTypes.SRID])
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "GeometryColumn":
        pyexasol_args = {PyexasolTypes.SRID: args[0]} if args else {}
        return cls.from_pyexasol(column_name, pyexasol_args)


class HashSizeUnit(Enum):
    BYTE = auto()
    BIT = auto()

    @classmethod
    def from_string(cls, name: str) -> "HashSizeUnit":
        for c in cls:
            if c.name == name:
                return c
        raise ValueError(f"Couldn't find HashSizeUnit with name '{name}'")


@dataclass(frozen=True, repr=True, eq=True)
class HashTypeColumn(Column):
    size: int = 16
    unit: HashSizeUnit = HashSizeUnit.BYTE

    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "HASHTYPE"

    @property
    def rendered(self) -> str:
        return f"{self.sql_name}({self.size} {self.unit.name})"

    @classmethod
    def simple(
        cls,
        name: str,
        size: int = 16,
        unit: HashSizeUnit = HashSizeUnit.BYTE,
    ) -> "HashTypeColumn":
        return cls(ColumnName(name), size, unit)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "HashTypeColumn":
        args = pyexasol_type_args(
            pyexasol_args,
            [PyexasolTypes.SIZE, PyexasolTypes.UNIT],
        )
        if unit := args.get("unit"):
            args["unit"] = HashSizeUnit.from_string(unit)
        if size := args.get("size"):
            # Data type HASHTYPE(n BYTE) -> Raw size in bytes = n + 1
            # see https://docs.exasol.com/db/latest/sql_references/data_types/data_type_size.htm#Otherdatatypes
            args["size"] = size - 1
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "HashTypeColumn":
        pyexasol_args = (
            {
                PyexasolTypes.SIZE: args[0] + 1,
                PyexasolTypes.UNIT: options,
            }
            if args
            else {}
        )
        return cls.from_pyexasol(column_name, pyexasol_args)


@dataclass(frozen=True, repr=True, eq=True)
class TimeStampColumn(Column):
    precision: int = 3
    local_time_zone: bool = False

    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "TIMESTAMP"

    @property
    def rendered(self) -> str:
        suffix = " WITH LOCAL TIME ZOME" if self.local_time_zone else ""
        return f"{self.sql_name}({self.precision}){suffix}"

    @classmethod
    def simple(
        cls,
        name: str,
        precision: int = 3,
        local_time_zone: bool = False,
    ) -> "TimeStampColumn":
        return cls(ColumnName(name), precision, local_time_zone)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "TimeStampColumn":
        args = pyexasol_type_args(
            pyexasol_args,
            {
                PyexasolTypes.PRECISION: "precision",
                PyexasolTypes.WITH_LOCAL_TIME_ZONE: "local_time_zone",
            },
        )
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "TimeStampColumn":
        pyexasol_args = {PyexasolTypes.PRECISION: args[0]} if args else {}
        if options:
            pyexasol_args[PyexasolTypes.WITH_LOCAL_TIME_ZONE] = True
        return cls.from_pyexasol(column_name, pyexasol_args)


@dataclass(frozen=True, repr=True, eq=True)
class VarCharColumn(Column):
    size: int
    charset: CharSet = CharSet.UTF8

    def __post_init__(self):
        check_dataclass_types(self)

    @classproperty
    def sql_name(cls) -> str:
        return "VARCHAR"

    @property
    def rendered(self) -> str:
        return f"{self.sql_name}({self.size}) CHARACTER SET {self.charset.name}"

    @classmethod
    def simple(
        cls,
        name: str,
        size: int,
        charset: CharSet = CharSet.UTF8,
    ) -> "VarCharColumn":
        return cls(ColumnName(name), size, charset)

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any],
    ) -> "VarCharColumn":
        args = pyexasol_type_args(pyexasol_args, CHAR_TYPE_ARGS)
        if charset := args.get("charset"):
            args["charset"] = CharSet.from_string(charset)
        return cls.simple(column_name, **args)

    @classmethod
    def from_sql(
        cls, column_name: str, args: list[int], options: str
    ) -> "VarCharColumn":
        pyexasol_args: dict[str, Any] = {
            PyexasolTypes.SIZE: args[0] if args else 2000000
        }
        if options:
            pyexasol_args[PyexasolTypes.CHARACTER_SET] = options
        return cls.from_pyexasol(column_name, pyexasol_args)
