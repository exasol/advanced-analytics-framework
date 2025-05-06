import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    DecimalColumn,
    TableBuilder,
    TableNameImpl,
)


def test_create_table_with_name_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_name(TableNameImpl("table")).build()


def test_create_table_with_columns_only_fail():
    with pytest.raises(TypeCheckError):
        column = TableBuilder().with_columns([DecimalColumn.simple("abc")]).build()


def test_create_table_with_name_and_columns():
    table = (
        TableBuilder()
        .with_name(TableNameImpl("table"))
        .with_columns([DecimalColumn.simple("column")])
        .build()
    )
    assert table.name.name == "table" and table.columns[0].name.name == "column"
