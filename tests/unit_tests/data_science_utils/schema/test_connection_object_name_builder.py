import pytest

from exasol.analytics.schema import (
    TableNameImpl,
    ConnectionObjectNameBuilder,
    ColumnName,
    ColumnNameBuilder,
)


def test_using_empty_constructor():
    with pytest.raises(TypeError):
        column_name = ConnectionObjectNameBuilder()


def test_using_constructor_name():
    connection_object_name = ConnectionObjectNameBuilder(name="connection").build()
    assert connection_object_name.name == "connection"


def test_using_create_name():
    connection_object_name = ConnectionObjectNameBuilder.create(name="connection")
    assert connection_object_name.name == "connection"
