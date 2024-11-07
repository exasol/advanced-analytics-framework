import pytest

from exasol.analytics.schema import (
    Column,
    ColumnType,
    ColumnNameBuilder,
)
from exasol.analytics.sql_executor.testing.mock_result_set import MockResultSet


def test_fetchall_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.fetchall()


def test_fetchone_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.fetchone()


def test_fetchmany_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.fetchmany()


def test_iter_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.__iter__()


def test_next_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.__next__()


def test_rowcount_rows_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.__next__()


def test_columns_columns_none_():
    result_set = MockResultSet()
    with pytest.raises(NotImplementedError):
        result_set.columns()


def test_for_loop():
    input = [("a", 1), ("b", 2), ("c", 4)]
    result_set = MockResultSet(rows=input)
    result = []
    for row in result_set:
        result.append(row)
    assert input == result


def test_fetchall():
    input = [("a", 1), ("b", 2), ("c", 4)]
    result_set = MockResultSet(rows=input)
    result = result_set.fetchall()
    assert input == result


def test_fetchmany():
    input = [("a", 1), ("b", 2), ("c", 4)]
    result_set = MockResultSet(rows=input)
    result = result_set.fetchmany(2)
    assert input[0:2] == result


def test_columns():
    input = [("a", 1), ("b", 2), ("c", 4)]
    columns = [Column(ColumnNameBuilder.create("t1"), ColumnType(name="VARCHAR(200000)")),
               Column(ColumnNameBuilder.create("t2"), ColumnType(name="INTEGER"))]
    result_set = MockResultSet(rows=input, columns=columns)
    assert columns == result_set.columns()


def test_rows_and_columns_different_length():
    input = [("a", 1), ("b", 2), ("c", 4)]
    columns = [Column(ColumnNameBuilder.create("t1"), ColumnType(name="VARCHAR(200000)"))]
    with pytest.raises(AssertionError):
        result_set = MockResultSet(rows=input, columns=columns)
