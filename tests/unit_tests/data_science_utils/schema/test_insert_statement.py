from inspect import cleandoc

import pytest

from exasol.analytics.schema import (
    ColumnName,
    InsertStatement,
    TableNameImpl,
    UnknownColumnError,
    decimal_column,
    timestamp_column,
    varchar_column,
)


def test_illegal_column():
    columns = [ColumnName("A")]
    testee = InsertStatement(columns)
    with pytest.raises(UnknownColumnError):
        testee.add_constants({"B": 1})


@pytest.mark.parametrize(
    "column_name, expected",
    [
        (ColumnName("C"), '"C"'),
        (ColumnName("C", TableNameImpl("T")), '"T"."C"'),
    ],
)
def test_references(column_name, expected):
    columns = [ColumnName("C")]
    testee = InsertStatement(columns).add_references(column_name)
    assert testee.values == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (1, "1"),
        ("a", "'a'"),
        (None, "NULL"),
    ],
)
def test_add_constants(value, expected):
    columns = [ColumnName("COL")]
    testee = InsertStatement(columns).add_constants({"COL": value})
    assert testee.values == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (1, "1"),
        ("CURRENT_SESSION", "CURRENT_SESSION"),
        (None, "NULL"),
    ],
)
def test_add_scalar_functions(value, expected):
    columns = [ColumnName("COL")]
    testee = InsertStatement(columns).add_scalar_functions({"COL": value})
    assert testee.values == expected


def test_insert_statement():
    columns = [ColumnName(s) for s in [ "LOG_TIMESTAMP", "NAME", "AGE", "ERR" ]]
    reference = ColumnName("ERR", TableNameImpl("TBL"))
    testee = (
        InsertStatement(columns, separator=", ")
        .add_scalar_functions({"LOG_TIMESTAMP": "SYSTIMESTAMP()"})
        .add_constants({"NAME": "Mary", "AGE": 21})
        .add_references(reference)
    )
    assert testee.columns == '''"LOG_TIMESTAMP", "AGE", "NAME", "ERR"'''
    assert testee.values == '''SYSTIMESTAMP(), 21, 'Mary', "TBL"."ERR"'''
