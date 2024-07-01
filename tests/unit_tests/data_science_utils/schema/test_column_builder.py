import pytest

from exasol_data_science_utils_python.schema.column import ColumnType
from exasol_data_science_utils_python.schema.column_builder import ColumnBuilder
from exasol_data_science_utils_python.schema.column_name_builder import ColumnNameBuilder


def test_create_column_with_name_only():
    with pytest.raises(TypeError):
        column = ColumnBuilder().with_name(ColumnNameBuilder.create("column")).build()


def test_create_column_with_type_only():
    with pytest.raises(TypeError):
        column = ColumnBuilder().with_type(type=ColumnType("INTEGER")).build()


def test_create_column_with_name_and_type():
    column = ColumnBuilder().with_name(ColumnNameBuilder.create("column")).with_type(type=ColumnType("INTEGER")).build()
    assert column.name.name == "column" and column.type.name == "INTEGER"
