import pyexasol
import pytest

from exasol.analytics.schema import (
    CharColumn,
    CharSet,
    Column,
    ColumnNameBuilder,
    DecimalColumn,
    HashSizeUnit,
    HashTypeColumn,
)
from exasol.analytics.sql_executor.pyexasol_impl import PyexasolSQLExecutor


@pytest.fixture()
def pyexasol_sql_executor():
    con = pyexasol.connect(dsn="localhost:8888", user="sys", password="exasol")
    # con = pyexasol.connect(dsn="192.168.124.221:8563", user="sys", password="exasol")
    yield PyexasolSQLExecutor(con)
    con.close()


RESULT_SET_INDEX = 0
EXPECTED_RESULT_INDEX = 1
EXPECTED_COLUMNS_INDEX = 2


@pytest.fixture()
def pyexasol_result_set(pyexasol_sql_executor):
    row_count = 100000
    expected_result = [(1, "a", "1.1", "bb")] * row_count
    expected_columns = [
        DecimalColumn.simple("c1", precision=1, scale=0),
        CharColumn.simple("c2", size=1, charset=CharSet.ASCII),
        DecimalColumn.simple("c3", precision=2, scale=1),
        HashTypeColumn.simple("c4", size=1, unit=HashSizeUnit.BYTE),
    ]
    result_set = pyexasol_sql_executor.execute(
        f"""SELECT 1 as "c1", 'a' as "c2", 1.1 as "c3", cast('bb' as HASHTYPE(1 BYTE)) as "c4"
        FROM VALUES BETWEEN 1 and {row_count} as t(i);"""
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
