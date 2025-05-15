from typing import Any

import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    BooleanColumn,
    CharColumn,
    CharSet,
    Column,
    ColumnName,
    ColumnType,
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

TEST_CASES_ARGUMENT_NAMES = ("subclass", "args", "sql_type", "sql_suffix")
TEST_CASES = [
    (BooleanColumn, {}, "BOOLEAN", ""),
    (
        CharColumn,
        {},
        "CHAR",
        "(1) UTF8",
    ),
    (
        CharColumn,
        {"size": 2, "charset": CharSet.ASCII},
        "CHAR",
        "(2) ASCII",
    ),
    (DateColumn, {}, "DATE", ""),
    (DecimalColumn, {}, "DECIMAL", "(18,0)"),
    (DecimalColumn, {"precision": 2}, "DECIMAL", "(2,0)"),
    (DecimalColumn, {"precision": 2, "scale": 1}, "DECIMAL", "(2,1)"),
    (DoublePrecisionColumn, {}, "DOUBLE PRECISION", ""),
    (GeometryColumn, {}, "GEOMETRY", "(0)"),
    (GeometryColumn, {"srid": 1}, "GEOMETRY", "(1)"),
    # Pyexasol output never contains unit and reports size in terms of
    # characters of the string representation which is 2 times the size in
    # BYTE specified during creation.
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
    (VarCharColumn, {"size": 2}, "VARCHAR", "(2) UTF8"),
    (
        VarCharColumn,
        {"size": 2, "charset": CharSet.ASCII},
        "VARCHAR",
        "(2) ASCII",
    ),
]

# --------------------------------------------------
# tests for class Column


def test_set_new_column_name_fail():
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


def test_column_from_sql_spec():
    actual = Column.from_sql_spec("H", "HASHTYPE(10 BYTE)")
    expected = HashTypeColumn.simple("H", unit=HashSizeUnit.BYTE, size=10)
    assert actual == expected


def test_column_from_pyexasol():
    pyexasol_spec = {
        "type": "TIMESTAMP",
        "precision": 4,
        "withLocalTimeZone": True,
    }
    actual = Column.from_pyexasol("TS", pyexasol_spec)
    expected = TimeStampColumn.simple("TS", precision=4, local_time_zone=True)
    assert actual == expected


# --------------------------------------------------
# tests for class ColumnType


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
        column_class(**args)


def test_varchar_without_size():
    with pytest.raises(TypeError, match="missing .* 'size'"):
        VarCharColumn()


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
        column_class(**args)


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


@pytest.mark.parametrize(TEST_CASES_ARGUMENT_NAMES, TEST_CASES)
def test_rendered(random_name, subclass, args, sql_type, sql_suffix):
    """
    This test compares the behavior of classes Column and ColumnType.
    """

    # instantiate the specified column type class in two ways
    plain = Column(ColumnName(random_name), subclass(**args))
    from_subclass = subclass.simple(random_name, **args)
    columns = [plain, from_subclass]

    # assert both results are equal
    assert columns[0] == columns[1]

    # and property rendered yields the expected result
    for col in columns:
        expected = f'"{random_name}" {sql_type}{sql_suffix}'
        assert col.rendered == expected


@pytest.mark.parametrize(
    "args, expected",
    [
        ({"type": "BOOLEAN"}, BooleanColumn()),
        ({"type": "CHAR"}, CharColumn()),
        ({"type": "CHAR", "size": 2}, CharColumn(size=2)),
        ({"type": "DATE"}, DateColumn()),
        ({"type": "DOUBLE"}, DoublePrecisionColumn()),
        ({"type": "DECIMAL"}, DecimalColumn()),
        (
            {"type": "DECIMAL", "precision": 10, "scale": 2},
            DecimalColumn(precision=10, scale=2),
        ),
        ({"type": "GEOMETRY"}, GeometryColumn()),
        ({"type": "GEOMETRY", "srid": 2}, GeometryColumn(srid=2)),
        ({"type": "HASHTYPE"}, HashTypeColumn()),
        (
            {"type": "HASHTYPE", "size": 16, "unit": "BIT"},
            HashTypeColumn(size=8, unit=HashSizeUnit.BIT),
        ),
        (
            {"type": "HASHTYPE", "size": 4},
            HashTypeColumn(size=2, unit=HashSizeUnit.BYTE),
        ),
        (
            {"type": "TIMESTAMP", "precision": 4, "withLocalTimeZone": True},
            TimeStampColumn(precision=4, local_time_zone=True),
        ),
        ({"type": "VARCHAR", "size": 2}, VarCharColumn(size=2)),
    ],
)
def test_column_type_from_pyexasol(args, expected):
    actual = ColumnType.from_pyexasol(args)
    assert actual == expected


def test_unknown_sql_type():
    with pytest.raises(UnsupportedSqlType):
        ColumnType.from_sql_spec("UNKNOWN")
