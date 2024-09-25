import pytest

from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_name_builder import ColumnNameBuilder
from exasol_data_science_utils_python.schema.connection_object_name_builder import ConnectionObjectNameBuilder
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl


def test_using_empty_constructor():
    with pytest.raises(TypeError):
        column_name = ConnectionObjectNameBuilder()


def test_using_constructor_name():
    connection_object_name = ConnectionObjectNameBuilder(name="connection").build()
    assert connection_object_name.name == "connection"


def test_using_create_name():
    connection_object_name = ConnectionObjectNameBuilder.create(name="connection")
    assert connection_object_name.name == "connection"
