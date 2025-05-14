import pytest

from exasol.analytics.schema import DecimalColumn


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

