import random
import string
from dataclasses import FrozenInstanceError
from typing import (
    Any,
    Iterator,
)

import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    BooleanColumn,
    CharColumn,
    CharSet,
    Column,
    ColumnName,
    DateColumn,
    DecimalColumn,
    DoublePrecisionColumn,
    GeometryColumn,
    HashSizeUnit,
    HashTypeColumn,
    TimeStampColumn,
    UnsupportedSqlType,
    VarCharColumn,
)
from exasol.analytics.schema.column_types import (
    PyexasolOption,
    SqlType,
)

TEST_CASES_ARGUMENT_NAMES = ("column_class", "args", "sql_type", "sql_suffix")
TEST_CASES = [
    # actual test cases
    # pyexasol output never contains unit and always assumes BYTE
    # size seems to be 2 times the size specified during creation.
    (BooleanColumn, {}, "BOOLEAN", ""),
    (
        CharColumn,
        {},
        "CHAR",
        "(1) CHARACTER SET UTF8",
    ),
    (
        CharColumn,
        {"size": 2, "charset": CharSet.ASCII},
        "CHAR",
        "(2) CHARACTER SET ASCII",
    ),
    (DateColumn, {}, "DATE", ""),
    (DecimalColumn, {}, "DECIMAL", "(18,0)"),
    (DecimalColumn, {"precision": 2}, "DECIMAL", "(2,0)"),
    (DecimalColumn, {"precision": 2, "scale": 1}, "DECIMAL", "(2,1)"),
    (DoublePrecisionColumn, {}, "DOUBLE PRECISION", ""),
    (GeometryColumn, {}, "GEOMETRY", "(0)"),
    (GeometryColumn, {"srid": 1}, "GEOMETRY", "(1)"),
    (HashTypeColumn, {}, "HASHTYPE", "(16 BYTE)"),
    (HashTypeColumn, {"size": 10}, "HASHTYPE", "(10 BYTE)"),
    (HashTypeColumn, {"unit": HashSizeUnit.BIT}, "HASHTYPE", "(16 BIT)"),
    (TimeStampColumn, {}, "TIMESTAMP", "(3)"),
    (
        TimeStampColumn,
        {"precision": 6, "local_time_zone": True},
        "TIMESTAMP",
        "(6) WITH LOCAL TIME ZOME",
    ),
    (VarCharColumn, {"size": 2}, "VARCHAR", "(2) CHARACTER SET UTF8"),
    (
        VarCharColumn,
        {"size": 2, "charset": CharSet.ASCII},
        "VARCHAR",
        "(2) CHARACTER SET ASCII",
    ),
]


@pytest.fixture
def random_name() -> str:
    return random.choice(string.ascii_letters.upper())


def test_set_new_name_fail():
    column = DecimalColumn.simple("abc")
    with pytest.raises(AttributeError) as c:
        column.name = "edf"


def test_equality():
    column1 = DecimalColumn.simple("abc")
    column2 = DecimalColumn.simple("abc")
    assert column1 == column2


def test_inequality_name():
    column1 = DecimalColumn.simple("abc")
    column2 = DecimalColumn.simple("def")
    assert column1 != column2


def test_inequality_precision():
    column1 = DecimalColumn.simple("abc", precision=2)
    column2 = DecimalColumn.simple("abc", precision=3)
    assert column1 != column2


def test_hash_equality():
    column1 = DecimalColumn.simple("abc", precision=2)
    column2 = DecimalColumn.simple("abc", precision=2)
    assert hash(column1) == hash(column2)


def test_hash_inequality_name():
    column1 = DecimalColumn.simple("abc")
    column2 = DecimalColumn.simple("def")
    assert hash(column1) != hash(column2)


def test_hash_inequality_precision():
    column1 = DecimalColumn.simple("abc", precision=2)
    column2 = DecimalColumn.simple("abc", precision=3)
    assert hash(column1) != hash(column2)


@pytest.mark.parametrize(
    "column_class, args",
    [
        (DecimalColumn, {"precision": "string"}),
        (DecimalColumn, {"scale": "string"}),
        (VarCharColumn, {"size": "string"}),
        (CharColumn, {"size": "string"}),
        (CharColumn, {"charset": 1}),
        (TimeStampColumn, {"local_time_zone": 1}),
        (GeometryColumn, {"srid": "string"}),
    ],
)
def test_arg_value_wrong_type(column_class, args):
    with pytest.raises(TypeCheckError):
        column_class(ColumnName("C"), **args)


def test_varchar_without_size():
    with pytest.raises(TypeError, match="missing .* 'size'"):
        VarCharColumn.simple()


@pytest.mark.parametrize(
    "column_class, args, message",
    [
        (CharColumn, {"size": 0}, r"size.* not in range\(1, 2001\)"),
        (CharColumn, {"size": 2001}, r"size.* not in range\(1, 2001\)"),
        (
            DecimalColumn,
            {"precision": 0},
            r"precision.* not in range\(1, 37\)",
        ),
        (
            DecimalColumn,
            {"scale": 40},
            r"scale.* not in range\(0, 37\)",
        ),
        (
            DecimalColumn,
            {"precision": 1, "scale": 2},
            "scale.* > precision",
        ),
        (
            HashTypeColumn,
            {"unit": HashSizeUnit.BIT, "size": 11},
            "multiple of 8",
        ),
        (
            HashTypeColumn,
            {"unit": HashSizeUnit.BIT, "size": 8193},
            r"size.* not in range\(8, 8193\)",
        ),
        (
            HashTypeColumn,
            {"unit": HashSizeUnit.BYTE, "size": 0},
            r"size.* not in range\(1, 1025\)",
        ),
        (TimeStampColumn, {"precision": 10}, r"precision.* not in range\(0, 10\)"),
        (TimeStampColumn, {"precision": -1}, r"precision.* not in range\(0, 10\)"),
        (VarCharColumn, {"size": 0}, r"size.* not in range\(1, 2000001\)"),
        (VarCharColumn, {"size": 2000001}, r"size.* not in range\(1, 2000001\)"),
    ],
)
def test_invalid_arguments(column_class, args, message):
    with pytest.raises(ValueError, match=message):
        column_class.simple("C", **args)


@pytest.mark.parametrize(TEST_CASES_ARGUMENT_NAMES, TEST_CASES)
def test_from_sql_spec(random_name, column_class, args, sql_type, sql_suffix):
    spec = f"{sql_type}{sql_suffix}".replace(" CHARACTER SET", "")
    actual = Column.from_sql_spec(random_name, spec)
    expected = column_class.simple(random_name, **args)
    assert actual == expected


def simulate_pyexasol_args(args: dict[str, Any]) -> dict[str, Any]:
    size = args.get("size", 16)
    unit = args.get("unit", HashSizeUnit.BYTE)
    return {
        "size": (size // 8) if unit == HashSizeUnit.BIT else size,
        "unit": HashSizeUnit.BYTE,
    }


@pytest.mark.parametrize(TEST_CASES_ARGUMENT_NAMES, TEST_CASES)
def test_for_create(random_name, column_class, args, sql_type, sql_suffix):
    # instantiate the specified column class in two ways
    columns = [
        column_class(ColumnName(random_name), **args),  # plain
        column_class.simple(random_name, **args),  # using class method simple()
    ]
    # assert both results are equal
    assert columns[0] == columns[1]
    # and property for_create yields the expected result
    for col in columns:
        expected = f'"{random_name}" {sql_type}{sql_suffix}'
        assert col.for_create == expected


@pytest.mark.parametrize(
    "column_name, args, expected",
    [
        ("B", {"type": "BOOLEAN"}, BooleanColumn.simple("B")),
        ("C", {"type": "CHAR"}, CharColumn.simple("C")),
        ("C", {"type": "CHAR", "size": 2}, CharColumn.simple("C", size=2)),
        ("A", {"type": "DATE"}, DateColumn.simple("A")),
        ("P", {"type": "DOUBLE"}, DoublePrecisionColumn.simple("P")),
        ("D", {"type": "DECIMAL"}, DecimalColumn.simple("D")),
        (
            "D",
            {"type": "DECIMAL", "precision": 10, "scale": 2},
            DecimalColumn.simple("D", precision=10, scale=2),
        ),
        ("G", {"type": "GEOMETRY"}, GeometryColumn.simple("G")),
        ("G", {"type": "GEOMETRY", "srid": 2}, GeometryColumn.simple("G", srid=2)),
        ("H", {"type": "HASHTYPE"}, HashTypeColumn.simple("H")),
        (
            "H",
            {"type": "HASHTYPE", "size": 16, "unit": "BIT"},
            HashTypeColumn.simple("H", size=8, unit=HashSizeUnit.BIT),
        ),
        (
            "H",
            {"type": "HASHTYPE", "size": 4},
            HashTypeColumn.simple("H", size=2, unit=HashSizeUnit.BYTE),
        ),
        (
            "T",
            {"type": "TIMESTAMP", "precision": 4, "withLocalTimeZone": True},
            TimeStampColumn.simple("T", precision=4, local_time_zone=True),
        ),
        ("V", {"type": "VARCHAR", "size": 2}, VarCharColumn.simple("V", size=2)),
    ],
)
def test_from_pyexasol(column_name, args, expected):
    actual = Column.from_pyexasol(column_name, args)
    assert actual == expected


def test_unknown_sql_type():
    with pytest.raises(UnsupportedSqlType):
        Column.from_sql_spec("name", "UNKNOWN")
