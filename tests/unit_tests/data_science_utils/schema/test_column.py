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
    VarCharColumn,
)
from exasol.analytics.schema.column_types import (
    PyexasolOption,
    SqlType,
)

TEST_CASES = [
    # argument names
    ("column_class", "args", "sql_type", "sql_suffix"),
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


def all_column_test_cases():
    argument_names = ",".join(TEST_CASES[0])
    actual_test_cases = TEST_CASES[1:]
    return pytest.mark.parametrize(argument_names, actual_test_cases)


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
            {"unit": HashSizeUnit.BIT, "size": 2},
            "multiple of 8",
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


@all_column_test_cases()
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


@all_column_test_cases()
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


# ------------------------------------------------------------


@pytest.mark.parametrize(
    "column_class, column_name, args",
    [
        (DecimalColumn, "D1", ()),
        (DecimalColumn, "D1", (10, 2)),
    ],
)
def test_str_constructor(column_class, column_name, args):
    plain = column_class(column_name, *args)
    with_column_name = column_class(ColumnName(column_name), *args)
    assert plain == with_column_name


@pytest.mark.parametrize(
    "column_class, arg",
    [
        (DecimalColumn, 123),
        (DecimalColumn, list("a")),
        (DecimalColumn, ValueError("")),
    ],
)
def test_constructor_invalid_type(column_class, arg):
    with pytest.raises(TypeCheckError):
        column_class(arg)


def test_str_constructor_set_name_fails():
    col = DecimalColumn("name")
    with pytest.raises(FrozenInstanceError):
        col.name = ColumnName("new_name")


# isort: off
import pyexasol


# isort: on
@pytest.mark.skip("local experiment to be removed before merge")
def test_i1():
    connection = pyexasol.connect(
        dsn="192.168.124.221:8563", user="SYS", password="exasol"
    )
    connection.execute("DROP TABLE IF EXISTS S.A")
    connection.execute(
        "CREATE TABLE S.A (H HASHTYPE(3 byte), T TIMESTAMP(3), V VARCHAR(10), C CHAR)"
    )
    stmt = connection.execute("SELECT * from S.A")
    print(f"{stmt.columns()}")
    for r in connection.execute("describe S.A"):
        print(f"{r}")
