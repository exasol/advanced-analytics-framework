import pytest

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.table_name_builder import TableNameBuilder
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl
from typeguard import TypeCheckError


def test_using_empty_constructor():
    with pytest.raises(TypeCheckError):
        table_name = TableNameBuilder().build()


def test_using_constructor_name_only():
    table_name = TableNameBuilder(name="table").build()
    assert table_name.name == "table" \
           and table_name.schema_name is None \
           and isinstance(table_name, TableName)


def test_using_constructor_schema():
    table_name = TableNameBuilder(name="table", schema=SchemaName("schema")).build()
    assert table_name.name == "table" \
           and table_name.schema_name.name is "schema" \
           and isinstance(table_name, TableName)


def test_using_with_name_only():
    table_name = TableNameBuilder().with_name("table").build()
    assert table_name.name == "table" \
           and table_name.schema_name is None \
           and isinstance(table_name, TableName)


def test_using_with_schema():
    table_name = TableNameBuilder().with_name("table").with_schema_name(SchemaName("schema")).build()
    assert table_name.name == "table" \
           and table_name.schema_name.name == "schema" \
           and isinstance(table_name, TableName)


def test_from_existing_using_with_schema():
    source_table_name = TableNameImpl("table")
    table_name = TableNameBuilder(table_name=source_table_name).with_schema_name(SchemaName("schema")).build()
    assert source_table_name.name == "table" \
           and source_table_name.schema_name is None \
           and table_name.name == "table" \
           and table_name.schema_name.name == "schema" \
           and isinstance(table_name, TableName)


def test_from_existing_using_with_name():
    source_table_name = TableNameImpl("table", SchemaName("schema"))
    table_name = TableNameBuilder(table_name=source_table_name).with_name("table1").build()
    assert source_table_name.name == "table" \
           and source_table_name.schema_name.name == "schema" \
           and table_name.schema_name.name == "schema" \
           and table_name.name == "table1" \
           and isinstance(table_name, TableName)


def test_from_existing_and_new_schema_in_constructor():
    source_table_name = TableNameImpl("table")
    table_name = TableNameBuilder(schema=SchemaName("schema"),
                                  table_name=source_table_name).build()
    assert source_table_name.name == "table" \
           and source_table_name.schema_name is None \
           and table_name.name == "table" \
           and table_name.schema_name.name == "schema" \
           and isinstance(table_name, TableName)


def test_from_existing_and_new_name_in_constructor():
    source_table_name = TableNameImpl("table", SchemaName("schema"))
    table_name = TableNameBuilder(name="table1",
                                  table_name=source_table_name).build()
    assert source_table_name.name == "table" \
           and source_table_name.schema_name.name == "schema" \
           and table_name.schema_name.name == "schema" \
           and table_name.name == "table1" \
           and isinstance(table_name, TableName)


def test_using_create_name_using_only_name():
    table_name = TableNameBuilder.create(name="table")
    assert table_name.name == "table" \
           and isinstance(table_name, TableName)


def test_using_create_name_using_schema():
    table_name = TableNameBuilder.create(name="table", schema=SchemaName("schema"))
    assert table_name.name == "table" \
           and table_name.schema_name.name == "schema" \
           and isinstance(table_name, TableName)
