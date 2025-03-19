from inspect import cleandoc

import pytest

from exasol.analytics.schema import (
    ColumnName,
    InsertStatement,
    TableNameImpl,
    decimal_column,
    timestamp_column,
    varchar_column,
)


@pytest.mark.parametrize(
    "column_name, expected",
    [
        (ColumnName("C"), '"C"'),
        (ColumnName("C", TableNameImpl("T")), '"T"."C"'),
    ],
)
def test_formatter_for_column_names(column_name, expected):
    columns = [
        varchar_column("C", size=200),
    ]
    testee = InsertStatement(columns).add_references(column_name)
    assert testee.values == expected


@pytest.mark.parametrize(
    "value, quote, expected",
    [
        (1, True, "1"),
        (1, False, "1"),
        ("a", True, "'a'"),
        ("a", False, "a"),
        (None, False, "NULL"),
        (None, True, "NULL"),
    ],
)
def test_column_value_for_scalars(value, quote, expected):
    columns = [varchar_column("COL", size=200)]
    testee = InsertStatement(columns).add({"COL": value}, quote)
    assert testee.values == expected


def test_column_values():
    columns = [
        timestamp_column("LOG_TIMESTAMP"),
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
        varchar_column("ERR", size=2000),
    ]
    reference = ColumnName("ERR", TableNameImpl("TBL"))
    testee = (
        InsertStatement(columns, separator=", ")
        .add({"LOG_TIMESTAMP": "SYSTIMESTAMP()"}, quote_values=False)
        .add({"NAME": "Mary", "AGE": 21}, quote_values=True)
        .add_references(reference)
    )
    assert testee.columns == '''"LOG_TIMESTAMP", "AGE", "NAME", "ERR"'''
    assert testee.values == '''SYSTIMESTAMP(), 21, 'Mary', "TBL"."ERR"'''
