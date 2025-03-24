import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    Column,
    ColumnNameBuilder,
    ColumnType,
    SizeUnit,
    decimal_column,
    hashtype_column,
    timestamp_column,
    varchar_column,
)


def test_set_new_type_fail():
    column = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    with pytest.raises(AttributeError) as c:
        column.type = "edf"


def test_set_new_name_fail():
    column = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    with pytest.raises(AttributeError) as c:
        column.name = "edf"


def test_wrong_types_in_constructor():
    with pytest.raises(TypeCheckError) as c:
        column = Column("abc", "INTEGER")


def test_equality():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    assert column1 == column2


def test_inequality_name():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("def"), ColumnType("INTEGER"))
    assert column1 != column2


def test_inequality_type():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("def"), ColumnType("VARCHAR"))
    assert column1 != column2


def test_hash_equality():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    assert hash(column1) == hash(column2)


def test_hash_inequality_name():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("def"), ColumnType("INTEGER"))
    assert hash(column1) != hash(column2)


def test_hash_inequality_type():
    column1 = Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))
    column2 = Column(ColumnNameBuilder.create("abc"), ColumnType("VARCHAR"))
    assert hash(column1) != hash(column2)


@pytest.mark.parametrize(
    "func, kwargs",
    [
        (decimal_column, {}),
        (decimal_column, {"precision": 20}),
        (decimal_column, {"precision": 20, "scale": 5}),
        (decimal_column, {"scale": 5}),
        (varchar_column, {"size": 200}),
        (varchar_column, {"size": 200, "characterSet": "ASCII"}),
        (timestamp_column, {}),
        (timestamp_column, {"precision": 2}),
    ],
)
def test_shortcut_functions(func, kwargs):
    """
    Test the shortcut functions varchar_column(), decimal_column(), and
    timestamp_column() to create instances of schema.Column.

    Pass the kwargs as specified in each test case and assert that the values
    of the attributes of Column.type of the created instance are matching the
    initial kwargs.
    """
    column = func("COLUMN_NAME", **kwargs)
    for attr, value in kwargs.items():
        assert getattr(column.type, attr) == value


@pytest.mark.parametrize ("bytes, bits, expected_error", [
    (2, 16, "bytes and bits are specified at the same time"),
    (None, 11, "bits is not a multiple of 8"),
])
def test_hashtype_column_invalid_args(bytes, bits, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        hashtype_column("cname", bytes=bytes, bits=bits)


@pytest.mark.parametrize ("bytes, bits, expected_size, expected_unit", [
    (2, None, 2, SizeUnit.BYTE),
    (None, 8, 8, SizeUnit.BIT),
    (None, None, 16, SizeUnit.BYTE),
])
def test_hashtype_column_valid_args(bytes, bits, expected_size, expected_unit):
    testee = hashtype_column("cname", bytes=bytes, bits=bits)
    assert testee.type.size == expected_size
    assert testee.type.unit == expected_unit
