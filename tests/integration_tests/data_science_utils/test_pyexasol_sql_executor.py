import pyexasol
import pytest

from exasol.analytics.schema import Column, ColumnNameBuilder, ColumnType
from exasol.analytics.sql_executor.pyexasol_impl import PyexasolSQLExecutor


@pytest.fixture()
def pyexasol_sql_executor():
    con = pyexasol.connect(dsn="localhost:8888", user="sys", password="exasol")
    yield PyexasolSQLExecutor(con)
    con.close()


RESULT_SET_INDEX = 0
EXPECTED_RESULT_INDEX = 1
EXPECTED_COLUMNS_INDEX = 2


@pytest.fixture()
def pyexasol_result_set(pyexasol_sql_executor):
    row_count = 100000
    expected_result = [(1, "a", "1.1")] * row_count
    expected_columns = [
        Column(
            ColumnNameBuilder.create("c1"),
            ColumnType(name="DECIMAL", precision=1, scale=0),
        ),
        Column(
            ColumnNameBuilder.create("c2"),
            ColumnType(name="CHAR", size=1, characterSet="ASCII"),
        ),
        Column(
            ColumnNameBuilder.create("c3"),
            ColumnType(name="DECIMAL", precision=2, scale=1),
        ),
    ]
    result_set = pyexasol_sql_executor.execute(
        f"""SELECT 1 as "c1", 'a' as "c2", 1.1 as "c3" FROM VALUES BETWEEN 1 and {row_count} as t(i);"""
    )
    return result_set, expected_result, expected_columns


def test_sql_executor(pyexasol_sql_executor):
    result_set = pyexasol_sql_executor.execute("SELECT 1")


def test_for_loop(pyexasol_result_set):
    input = pyexasol_result_set[EXPECTED_RESULT_INDEX]
    result = [row for row in pyexasol_result_set[RESULT_SET_INDEX]]
    assert input == result


def test_fetchall(pyexasol_result_set):
    input = pyexasol_result_set[EXPECTED_RESULT_INDEX]
    result = pyexasol_result_set[RESULT_SET_INDEX].fetchall()
    assert input == result


def test_fetchmany(pyexasol_result_set):
    input = pyexasol_result_set[EXPECTED_RESULT_INDEX]
    result = pyexasol_result_set[RESULT_SET_INDEX].fetchmany(2)
    assert input[0:2] == result


def test_columns(pyexasol_result_set):
    expected_columns = pyexasol_result_set[EXPECTED_COLUMNS_INDEX]
    actual_columns = pyexasol_result_set[RESULT_SET_INDEX].columns()
    assert expected_columns == actual_columns
