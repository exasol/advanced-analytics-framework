import pytest

from exasol.analytics.schema import (
    Column,
    ColumnNameBuilder,
    ColumnType,
    TableBuilder,
    TableNameImpl,
)
from typeguard import TypeCheckError


def test_create_table_with_name_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_name(TableNameImpl("table")).build()


def test_create_table_with_columns_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_columns([Column(ColumnNameBuilder.create("abc"), ColumnType("INTEGER"))]).build()


def test_create_table_with_name_and_columns():
    table = TableBuilder().with_name(TableNameImpl("table")).with_columns(
        [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))]).build()
    assert table.name.name == "table" and table.columns[0].name.name == "column"
