import random
import string

import pytest
from typeguard import (
    Any,
    TypeCheckError,
)

from exasol.analytics.schema.column import (
    BooleanColumn,
    CharColumn,
    CharSet,
    Column,
    DateColumn,
    DecimalColumn,
    DoublePrecisionColumn,
    GeometryColumn,
    HashSizeUnit,
    HashTypeColumn,
    PyexasolTypes,
    SqlType,
    TimeStampColumn,
    VarCharColumn,
)
from exasol.analytics.schema.column_name import ColumnName

TEST_CASES = [
    (BooleanColumn, {}, "BOOLEAN", ""),
    (CharColumn, {}, "CHAR", "(1) CHARACTER SET UTF8"),
    (
        CharColumn,
        {"size": 2, "charset": CharSet.ASCII},
        "CHAR",
        "(2) CHARACTER SET ASCII",
    ),
    (DateColumn, {}, "DATE", ""),
    (DecimalColumn, {}, "DECIMAL", "(18,0)"),
    (DecimalColumn, {"precision": 2}, "DECIMAL", "(2,0)"),
    (DecimalColumn, {"scale": 1}, "DECIMAL", "(18,1)"),
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


@pytest.mark.parametrize("column_class, args, sql_type, sql_suffix", TEST_CASES)
def test_for_create(column_class, args, sql_type, sql_suffix):
    # select a random column name
    name = random.choice(string.ascii_letters.upper())
    # instantiate the specified column class, once plain and once using class
    # method simple()
    columns = [
        column_class(ColumnName(name), **args),
        column_class.simple(name, **args),
    ]
    # assert both are equal
    assert columns[0] == columns[1]
    # and property for_create yields the expected result
    for col in columns:
        assert col.for_create == f'"{name}" {sql_type}{sql_suffix}'


@pytest.mark.parametrize("column_class, args, sql_type, sql_suffix", TEST_CASES)
def test_from_pyexasol(column_class, args, sql_type, sql_suffix):
    """
    Verify Column.from_pyexasol() returns the same as <c>.simple() of the
    resp. subclass <c> of Column.
    """
    name = random.choice(string.ascii_letters.upper())
    mapping = {
        "local_time_zone": PyexasolTypes.WITH_LOCAL_TIME_ZONE,
        "charset": PyexasolTypes.CHARACTER_SET,
    }

    def sql_value(key: str, value: Any) -> Any:
        if sql_type == "HASHTYPE":
            if key == "size":
                return value + 1
            if key == "unit":
                return value.name
            return value
        if key == "charset":
            return value.name
        return value

    sql_type_args = {mapping.get(k, k): sql_value(k, v) for k, v in args.items()}
    actual = Column.from_pyexasol(name, sql_type, sql_type_args)
    expected = column_class.simple(name, **args)
    assert actual == expected


@pytest.mark.parametrize(
    "column_class, args, expected_error",
    [
        (VarCharColumn, {}, "missing 1 .* 'size'"),
    ],
)
def test_insufficient_parameters(column_class, args, expected_error):
    with pytest.raises(TypeError, match=expected_error):
        column_class(ColumnName("C"), **args)


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
def test_wrong_type(column_class, args):
    with pytest.raises(TypeCheckError):
        column_class(ColumnName("C"), **args)


@pytest.mark.parametrize(
    "sql_type, expected",
    [
        ("BOOLEAN", BooleanColumn.simple("B")),
        ("CHAR", CharColumn.simple("V", size=1)),
        ("CHAR(10) ASCII", CharColumn.simple("V", size=10, charset=CharSet.ASCII)),
        ("CHAR(10)", CharColumn.simple("V", size=10)),
        ("DATE", DateColumn.simple("A")),
        ("DECIMAL", DecimalColumn.simple("D")),
        ("INTEGER", DecimalColumn.simple("D")),
        ("FLOAT", DoublePrecisionColumn.simple("D")),
        ("DECIMAL(10)", DecimalColumn.simple("D", precision=10)),
        ("DECIMAL(2,1)", DecimalColumn.simple("D", precision=2, scale=1)),
        ("DOUBLE PRECISION", DoublePrecisionColumn.simple("P")),
        ("DOUBLE", DoublePrecisionColumn.simple("P")),
        ("GEOMETRY", GeometryColumn.simple("G")),
        ("GEOMETRY(2)", GeometryColumn.simple("G", srid=2)),
        (
            "HASHTYPE(1 BYTE)",
            HashTypeColumn.simple("H", size=1, unit=HashSizeUnit.BYTE),
        ),
        ("TIMESTAMP", TimeStampColumn.simple("T")),
        (
            "TIMESTAMP(3) WITH LOCAL TIME ZOME",
            TimeStampColumn.simple("T", precision=3, local_time_zone=True),
        ),
        ("VARCHAR", VarCharColumn.simple("V", size=2000000)),
        (
            "VARCHAR(10) ASCII",
            VarCharColumn.simple("V", size=10, charset=CharSet.ASCII),
        ),
        ("VARCHAR(10)", VarCharColumn.simple("V", size=10)),
    ],
)
def test_from_sql_type(sql_type: str, expected: Column):
    actual = Column.from_sql_type(expected.name.name, sql_type)
    assert actual == expected
