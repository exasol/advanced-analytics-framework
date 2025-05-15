from typing import Any

import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    BooleanType,
    CharSet,
    CharType,
    Column,
    ColumnName,
    ColumnType,
    DateType,
    DecimalType,
    DoublePrecisionType,
    GeometryType,
    HashSizeUnit,
    HashTypeType,
    TimeStampType,
    UnsupportedSqlType,
    VarCharType,
    boolean_column,
    char_column,
    date_column,
    decimal_column,
    double_column,
    geometry_column,
    hashtype_column,
    timestamp_column,
    varchar_column,
)

TEST_CASES_ARGUMENT_NAMES = ("subclass", "args", "sql_type", "sql_suffix")
TEST_CASES = [
    (BooleanType, {}, "BOOLEAN", ""),
    (
        CharType,
        {},
        "CHAR",
        "(1) UTF8",
    ),
    (
        CharType,
        {"size": 2, "charset": CharSet.ASCII},
        "CHAR",
        "(2) ASCII",
    ),
    (DateType, {}, "DATE", ""),
    (DecimalType, {}, "DECIMAL", "(18,0)"),
    (DecimalType, {"precision": 2}, "DECIMAL", "(2,0)"),
    (DecimalType, {"precision": 2, "scale": 1}, "DECIMAL", "(2,1)"),
    (DoublePrecisionType, {}, "DOUBLE PRECISION", ""),
    (GeometryType, {}, "GEOMETRY", "(0)"),
    (GeometryType, {"srid": 1}, "GEOMETRY", "(1)"),
    # Pyexasol output never contains unit and reports size in terms of
    # characters of the string representation which is 2 times the size in
    # BYTE specified during creation.
    (HashTypeType, {}, "HASHTYPE", "(16 BYTE)"),
    (HashTypeType, {"size": 10}, "HASHTYPE", "(10 BYTE)"),
    (HashTypeType, {"unit": HashSizeUnit.BIT}, "HASHTYPE", "(16 BIT)"),
    (TimeStampType, {}, "TIMESTAMP", "(3)"),
    (
        TimeStampType,
        {"precision": 6, "local_time_zone": True},
        "TIMESTAMP",
        "(6) WITH LOCAL TIME ZOME",
    ),
    (VarCharType, {"size": 2}, "VARCHAR", "(2) UTF8"),
    (
        VarCharType,
        {"size": 2, "charset": CharSet.ASCII},
        "VARCHAR",
        "(2) ASCII",
    ),
]

# --------------------------------------------------
# tests for class Column


def test_set_new_column_name_fail():
    column = decimal_column("abc")
    with pytest.raises(AttributeError) as c:
        column.name = "edf"


def test_equality():
    column1 = decimal_column("abc")
    column2 = decimal_column("abc")
    assert column1 == column2


def test_inequality_name():
    column1 = decimal_column("abc")
    column2 = decimal_column("def")
    assert column1 != column2


def test_inequality_precision():
    column1 = decimal_column("abc", precision=2)
    column2 = decimal_column("abc", precision=3)
    assert column1 != column2


def test_hash_equality():
    column1 = decimal_column("abc", precision=2)
    column2 = decimal_column("abc", precision=2)
    assert hash(column1) == hash(column2)


def test_hash_inequality_name():
    column1 = decimal_column("abc")
    column2 = decimal_column("def")
    assert hash(column1) != hash(column2)


def test_hash_inequality_precision():
    column1 = decimal_column("abc", precision=2)
    column2 = decimal_column("abc", precision=3)
    assert hash(column1) != hash(column2)


def test_column_from_sql_spec():
    actual = Column.from_sql_spec("H", "HASHTYPE(10 BYTE)")
    expected = hashtype_column("H", unit=HashSizeUnit.BYTE, size=10)
    assert actual == expected


def test_column_from_pyexasol():
    pyexasol_spec = {
        "type": "TIMESTAMP",
        "precision": 4,
        "withLocalTimeZone": True,
    }
    actual = Column.from_pyexasol("TS", pyexasol_spec)
    expected = timestamp_column("TS", precision=4, local_time_zone=True)
    assert actual == expected


# --------------------------------------------------
# tests for class ColumnType


@pytest.mark.parametrize(
    "column_class, args",
    [
        (DecimalType, {"precision": "string"}),
        (DecimalType, {"scale": "string"}),
        (VarCharType, {"size": "string"}),
        (CharType, {"size": "string"}),
        (CharType, {"charset": 1}),
        (TimeStampType, {"local_time_zone": 1}),
        (GeometryType, {"srid": "string"}),
    ],
)
def test_arg_value_wrong_type(column_class, args):
    with pytest.raises(TypeCheckError):
        column_class(**args)


def test_varchar_without_size():
    with pytest.raises(TypeError, match="missing .* 'size'"):
        VarCharType()


@pytest.mark.parametrize(
    "subclass, args, message",
    [
        (CharType, {"size": 0}, r"size.* not in range\(1, 2001\)"),
        (CharType, {"size": 2001}, r"size.* not in range\(1, 2001\)"),
        (
            DecimalType,
            {"precision": 0},
            r"precision.* not in range\(1, 37\)",
        ),
        (
            DecimalType,
            {"scale": 40},
            r"scale.* not in range\(0, 37\)",
        ),
        (
            DecimalType,
            {"precision": 1, "scale": 2},
            "scale.* > precision",
        ),
        (
            HashTypeType,
            {"unit": HashSizeUnit.BIT, "size": 11},
            "multiple of 8",
        ),
        (
            HashTypeType,
            {"unit": HashSizeUnit.BIT, "size": 8193},
            r"size.* not in range\(8, 8193\)",
        ),
        (
            HashTypeType,
            {"unit": HashSizeUnit.BYTE, "size": 0},
            r"size.* not in range\(1, 1025\)",
        ),
        (TimeStampType, {"precision": 10}, r"precision.* not in range\(0, 10\)"),
        (TimeStampType, {"precision": -1}, r"precision.* not in range\(0, 10\)"),
        (VarCharType, {"size": 0}, r"size.* not in range\(1, 2000001\)"),
        (VarCharType, {"size": 2000001}, r"size.* not in range\(1, 2000001\)"),
    ],
)
def test_invalid_arguments(subclass, args, message):
    with pytest.raises(ValueError, match=message):
        subclass(**args)


@pytest.mark.parametrize(TEST_CASES_ARGUMENT_NAMES, TEST_CASES)
def test_column_type_from_sql_spec(subclass, args, sql_type, sql_suffix):
    spec = f"{sql_type}{sql_suffix}"
    actual = ColumnType.from_sql_spec(spec)
    expected = subclass(**args)
    assert actual == expected


def simulate_pyexasol_args(args: dict[str, Any]) -> dict[str, Any]:
    size = args.get("size", 16)
    unit = args.get("unit", HashSizeUnit.BYTE)
    return {
        "size": (size // 8) if unit == HashSizeUnit.BIT else size,
        "unit": HashSizeUnit.BYTE,
    }


@pytest.fixture
def random_name() -> str:
    import random
    import string

    return random.choice(string.ascii_letters.upper())


CONVENIENCE_METHODS = {
    BooleanType: boolean_column,
    CharType: char_column,
    DateType: date_column,
    DecimalType: decimal_column,
    DoublePrecisionType: double_column,
    GeometryType: geometry_column,
    HashTypeType: hashtype_column,
    TimeStampType: timestamp_column,
    VarCharType: varchar_column,
}


@pytest.mark.parametrize(TEST_CASES_ARGUMENT_NAMES, TEST_CASES)
def test_rendered(random_name, subclass, args, sql_type, sql_suffix):
    """
    This test compares the behavior of classes Column and ColumnType.
    """

    def create_via_convenience_method(name: str, **args: Any) -> ColumnType:
        method = CONVENIENCE_METHODS[subclass]
        return method(name, **args)

    # instantiate the specified column type class in two ways
    plain = Column(ColumnName(random_name), subclass(**args))
    via_convenience_method = create_via_convenience_method(random_name, **args)
    columns = [plain, via_convenience_method]

    # assert both results are equal
    assert columns[0] == columns[1]

    # and property rendered yields the expected result
    for col in columns:
        expected = f'"{random_name}" {sql_type}{sql_suffix}'
        assert col.rendered == expected


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"type": "BOOLEAN"}, BooleanType()),
        ({"type": "CHAR"}, CharType()),
        ({"type": "CHAR", "size": 2}, CharType(size=2)),
        ({"type": "DATE"}, DateType()),
        ({"type": "DOUBLE"}, DoublePrecisionType()),
        ({"type": "DECIMAL"}, DecimalType()),
        (
            {"type": "DECIMAL", "precision": 10, "scale": 2},
            DecimalType(precision=10, scale=2),
        ),
        ({"type": "GEOMETRY"}, GeometryType()),
        ({"type": "GEOMETRY", "srid": 2}, GeometryType(srid=2)),
        ({"type": "HASHTYPE"}, HashTypeType()),
        (
            {"type": "HASHTYPE", "size": 16, "unit": "BIT"},
            HashTypeType(size=8, unit=HashSizeUnit.BIT),
        ),
        (
            {"type": "HASHTYPE", "size": 4},
            HashTypeType(size=2, unit=HashSizeUnit.BYTE),
        ),
        (
            {"type": "TIMESTAMP", "precision": 4, "withLocalTimeZone": True},
            TimeStampType(precision=4, local_time_zone=True),
        ),
        ({"type": "VARCHAR", "size": 2}, VarCharType(size=2)),
    ],
)
def test_column_type_from_pyexasol(args, expected):
    actual = ColumnType.from_pyexasol(args)
    assert actual == expected


def test_unknown_sql_type():
    with pytest.raises(UnsupportedSqlType):
        ColumnType.from_sql_spec("UNKNOWN")
