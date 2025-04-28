# do not from __future__ import annotations
# as this breaks typeguard checks

import re
from abc import (
    ABC,
    ABCMeta,
    abstractmethod,
)
from dataclasses import dataclass
from enum import (
    Enum,
    auto,
)
from typing import (
    Any,
    Callable,
    Optional,
    Protocol,
)

import typeguard

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.column_types import (
    CharSet,
    ColumnTypeSource,
    PyexasolMapping,
    PyexasolOption,
    SqlType,
)
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


class classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


class ColumnClass(ABCMeta):
    @abstractmethod
    def sql_names(self) -> list[str]: ...

    @abstractmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "Column": ...

    def pyexasol_mapping(self) -> PyexasolMapping:
        """
        Default PyexasolMapping inherited as @classmethod by all classes
        declaring ColumnClass as metaclass, overriden in some classes.
        """
        return PyexasolMapping(int_keys=[], modifier_key=None)

    @classmethod
    def from_sql_name(cls, sql_name: str) -> "ColumnClass":
        all: list[ColumnClass] = [
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
        return next(c for c in all if sql_name in c.sql_names())


@dataclass(frozen=True, repr=True, eq=True)
class Column(ABC):
    name: ColumnName

    def __post_init__(self):
        """
        As convenience Column supports specifying name as a simple str,
        and __post_init__() instantiates ColumnName.
        """
        # If this approach is accepted, then we can remove classmethod
        # simple() from the subclasses of Column which simplifies
        # implementation and reduces overhead.
        if isinstance(self.name, str):
            super().__setattr__("name", ColumnName(self.name))
        check_dataclass_types(self)

    @property
    def for_create(self) -> str:
        return f"{self.name.fully_qualified} {self.sql_spec(for_create=True)}"

    @property
    def rendered(self) -> str:
        return self.sql_spec(for_create=False)

    @abstractmethod
    def sql_spec(self, for_create: bool) -> str: ...

    @classproperty
    def sql_name(self):
        return self.sql_names()[0]

    @classmethod
    def check_arg(cls, name: str, value: int, allowed: range):
        if value not in allowed:
            raise ValueError(f"{cls.__name__} {name}={value} not in {allowed}.")

    @classmethod
    def from_pyexasol(
        cls,
        column_name: str,
        pyexasol_args: dict[str, Any] = {},
    ) -> "Column":
        sql_type_name = pyexasol_args["type"]
        column_class = ColumnClass.from_sql_name(sql_type_name)
        sql_type = SqlType.from_pyexasol(
            pyexasol_args,
            column_class.pyexasol_mapping(),
        )
        return column_class.from_sql(column_name, sql_type)

    @classmethod
    def from_sql_spec(cls, column_name: str, spec: str) -> "Column":
        """
        spec, e.g. "VARCHAR(100) ASCII" is also available in
        exa.meta.input_columns[0].sql_type
        """
        sql_type = SqlType.from_string(spec)
        column_class = ColumnClass.from_sql_name(sql_type.name)
        return column_class.from_sql(column_name, sql_type)


@dataclass(frozen=True, repr=True, eq=True)
class BooleanColumn(Column, metaclass=ColumnClass):
    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)

    def sql_spec(self, for_create: bool = False) -> str:
        return self.sql_name

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["BOOLEAN"]

    @classmethod
    def simple(cls, name: str) -> "BooleanColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "BooleanColumn":
        return cls.simple(column_name)


@dataclass(frozen=True, repr=True, eq=True)
class CharColumn(Column, metaclass=ColumnClass):
    size: int = 1
    charset: CharSet = CharSet.UTF8

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)
        self.check_arg("size", self.size, range(1, 2001))

    def sql_spec(self, for_create: bool = False) -> str:
        return f"{self.sql_name}({self.size}) CHARACTER SET {self.charset.name}"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["CHAR"]

    @classmethod
    def simple(
        cls,
        name: str,
        size: int = 1,
        charset: CharSet = CharSet.UTF8,
    ) -> "CharColumn":
        return cls(ColumnName(name), size, charset)

    @classmethod
    def pyexasol_mapping(cls):
        return PyexasolMapping(
            int_keys=[PyexasolOption.SIZE], modifier_key=PyexasolOption.CHARACTER_SET
        )

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "CharColumn":
        args = sql_type.char_type_args
        return cls.simple(column_name, **args)


@dataclass(frozen=True, repr=True, eq=True)
class DateColumn(Column, metaclass=ColumnClass):
    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)

    def sql_spec(self, for_create: bool = False) -> str:
        return self.sql_name

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["DATE"]

    @classmethod
    def simple(cls, name: str) -> "DateColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "DateColumn":
        return cls.simple(column_name)


@dataclass(frozen=True, repr=True, eq=True)
class DecimalColumn(Column, metaclass=ColumnClass):
    precision: int = 18
    scale: int = 0

    def sql_spec(self, for_create: bool = False) -> str:
        return f"{self.sql_name}({self.precision},{self.scale})"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["DECIMAL", "INTEGER"]

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)
        self.check_arg("precision", self.precision, range(1, 37))
        self.check_arg("scale", self.scale, range(0, 37))
        if self.scale > self.precision:
            raise ValueError(
                f"DecimalColumn scale must be ≤ precision but"
                f" scale={self.scale} > precision={self.precision}."
            )

    @classmethod
    def simple(cls, name: str, precision: int = 18, scale: int = 0) -> "DecimalColumn":
        return cls(ColumnName(name), precision, scale)

    @classmethod
    def pyexasol_mapping(cls) -> PyexasolMapping:
        return PyexasolMapping(
            int_keys=[PyexasolOption.PRECISION, PyexasolOption.SCALE]
        )

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "DecimalColumn":
        args = sql_type.int_dict(keys=["precision", "scale"])
        return cls.simple(column_name, **args)


@dataclass(frozen=True, repr=True, eq=True)
class DoublePrecisionColumn(Column, metaclass=ColumnClass):
    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)

    def sql_spec(self, for_create: bool = False) -> str:
        return self.sql_name

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["DOUBLE PRECISION", "DOUBLE", "FLOAT"]

    @classmethod
    def simple(cls, name: str) -> "DoublePrecisionColumn":
        return cls(ColumnName(name))

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "DoublePrecisionColumn":
        return cls.simple(column_name)


@dataclass(frozen=True, repr=True, eq=True)
class GeometryColumn(Column, metaclass=ColumnClass):
    srid: int = 0
    "Spatial reference identifier"

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)

    def sql_spec(self, for_create: bool = False) -> str:
        return f"{self.sql_name}({self.srid})"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["GEOMETRY"]

    @classmethod
    def simple(cls, name: str, srid: int = 0) -> "GeometryColumn":
        return cls(ColumnName(name), srid)

    @classmethod
    def pyexasol_mapping(cls):
        return PyexasolMapping(int_keys=[PyexasolOption.SRID])

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "GeometryColumn":
        args = sql_type.int_dict(keys=["srid"])
        return cls.simple(column_name, **args)


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
class HashTypeColumn(Column, metaclass=ColumnClass):
    size: int = 16
    unit: HashSizeUnit = HashSizeUnit.BYTE

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)
        if self.unit == HashSizeUnit.BIT and self.size % 8:
            raise ValueError(
                "HashTypeColumn with unit BIT and"
                f" size not a multiple of 8: size={self.size}."
            )

    def sql_spec(self, for_create: bool = False) -> str:
        return f"{self.sql_name}({self.size} {self.unit.name})"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["HASHTYPE"]

    @classmethod
    def simple(
        cls,
        name: str,
        size: int = 16,
        unit: HashSizeUnit = HashSizeUnit.BYTE,
    ) -> "HashTypeColumn":
        return cls(ColumnName(name), size, unit)

    @classmethod
    def pyexasol_mapping(cls):
        return PyexasolMapping(
            int_keys=[PyexasolOption.SIZE],
            modifier_key=PyexasolOption.UNIT,
        )

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "HashTypeColumn":
        if sql_type.source == ColumnTypeSource.PYEXASOL:
            int_args = sql_type.int_args or [32]
            # For data type HASHTYPE(n BYTE) web-socket-api currently returns
            # n * 2 differing from documentation.
            #
            # References
            # * https://docs.exasol.com/db/latest/sql_references/data_types/data_type_size.htm#Otherdatatypes
            # * https://github.com/exasol/pyexasol/issues/189
            # * https://github.com/exasol/websocket-api/blob/master/python/EXASOL/__init__.py#L206
            size = int_args[0] // 2
            sql_type = SqlType(sql_type.name, [size], sql_type.modifier)

        args = sql_type.int_dict(keys=["size"])
        if sql_type.modifier:
            args["unit"] = HashSizeUnit.from_string(sql_type.modifier)
        return cls.simple(column_name, **args)


@dataclass(frozen=True, repr=True, eq=True)
class TimeStampColumn(Column, metaclass=ColumnClass):
    precision: int = 3
    local_time_zone: bool = False

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)
        self.check_arg("precision", self.precision, range(0, 10))

    def sql_spec(self, for_create: bool = False) -> str:
        suffix = " WITH LOCAL TIME ZOME" if self.local_time_zone else ""
        return f"{self.sql_name}({self.precision}){suffix}"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["TIMESTAMP"]

    @classmethod
    def simple(
        cls,
        name: str,
        precision: int = 3,
        local_time_zone: bool = False,
    ) -> "TimeStampColumn":
        return cls(ColumnName(name), precision, local_time_zone)

    @classmethod
    def pyexasol_mapping(cls):
        modifier_getter = lambda args: (
            "WITH_LOCAL_TIME_ZONE"
            if args.get(PyexasolOption.WITH_LOCAL_TIME_ZONE.value, False)
            else ""
        )
        return PyexasolMapping(
            int_keys=[PyexasolOption.PRECISION],
            modifier_getter=modifier_getter,
        )

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "TimeStampColumn":
        args = sql_type.int_dict(keys=["precision"])
        if sql_type.modifier:
            args["local_time_zone"] = True
        return cls.simple(column_name, **args)


@dataclass(frozen=True, repr=True, eq=True)
class VarCharColumn(Column, metaclass=ColumnClass):
    size: int
    charset: CharSet = CharSet.UTF8

    def __post_init__(self):
        super().__post_init__()
        check_dataclass_types(self)
        self.check_arg("size", self.size, range(1, 2000001))

    def sql_spec(self, for_create: bool = False) -> str:
        charset_spec = " CHARACTER SET" if for_create else ""
        return f"{self.sql_name}({self.size}){charset_spec} {self.charset.name}"

    @classmethod
    def sql_names(cls) -> list[str]:
        return ["VARCHAR"]

    @classmethod
    def simple(
        cls,
        name: str,
        size: int,
        charset: CharSet = CharSet.UTF8,
    ) -> "VarCharColumn":
        return cls(ColumnName(name), size, charset)

    @classmethod
    def pyexasol_mapping(cls):
        return PyexasolMapping(
            int_keys=[PyexasolOption.SIZE], modifier_key=PyexasolOption.CHARACTER_SET
        )

    @classmethod
    def from_sql(cls, column_name: str, sql_type: SqlType) -> "VarCharColumn":
        args = sql_type.char_type_args
        return cls.simple(column_name, **args)
