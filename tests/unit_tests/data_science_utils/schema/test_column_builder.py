import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    ColumnBuilder,
    ColumnNameBuilder,
    ColumnType,
)


def test_create_column_with_name_only():
    with pytest.raises(TypeCheckError):
        column = ColumnBuilder().with_name(ColumnNameBuilder.create("column")).build()


def test_create_column_with_type_only():
    with pytest.raises(TypeCheckError):
        column = ColumnBuilder().with_type(type=ColumnType("INTEGER")).build()


def test_create_column_with_name_and_type():
    column = (
        ColumnBuilder()
        .with_name(ColumnNameBuilder.create("column"))
        .with_type(type=ColumnType("INTEGER"))
        .build()
    )
    assert column.name.name == "column" and column.type.name == "INTEGER"
