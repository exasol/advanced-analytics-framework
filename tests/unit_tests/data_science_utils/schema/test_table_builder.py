import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    TableBuilder,
    TableNameImpl,
    decimal_column,
)


def test_create_table_with_name_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_name(TableNameImpl("table")).build()


def test_create_table_with_columns_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_columns([decimal_column("abc")]).build()


def test_create_table_with_name_and_columns():
    table = (
        TableBuilder()
        .with_name(TableNameImpl("table"))
        .with_columns([decimal_column("column")])
        .build()
    )
    assert table.name.name == "table" and table.columns[0].name.name == "column"
