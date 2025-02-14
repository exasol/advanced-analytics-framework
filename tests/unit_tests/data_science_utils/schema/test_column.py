import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    Column,
    ColumnNameBuilder,
    ColumnType,
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
