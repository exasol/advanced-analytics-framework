import pytest

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.udf_name import UDFName
from exasol_data_science_utils_python.schema.udf_name_builder import UDFNameBuilder
from exasol_data_science_utils_python.schema.udf_name_impl import UDFNameImpl


def test_using_empty_constructor():
    with pytest.raises(TypeError):
        udf_name = UDFNameBuilder().build()


def test_using_constructor_name_only():
    udf_name = UDFNameBuilder(name="udf").build()
    assert udf_name.name == "udf" \
           and udf_name.schema_name is None \
           and isinstance(udf_name, UDFName)


def test_using_constructor_schema():
    udf_name = UDFNameBuilder(name="udf", schema=SchemaName("schema")).build()
    assert udf_name.name == "udf" \
           and udf_name.schema_name.name is "schema" \
           and isinstance(udf_name, UDFName)


def test_using_with_name_only():
    udf_name = UDFNameBuilder().with_name("udf").build()
    assert udf_name.name == "udf" \
           and udf_name.schema_name is None \
           and isinstance(udf_name, UDFName)


def test_using_with_schema():
    udf_name = UDFNameBuilder().with_name("udf").with_schema_name(SchemaName("schema")).build()
    assert udf_name.name == "udf" \
           and udf_name.schema_name.name == "schema" \
           and isinstance(udf_name, UDFName)


def test_from_existing_using_with_schema():
    source_udf_name = UDFNameImpl("udf")
    udf_name = UDFNameBuilder(udf_name=source_udf_name).with_schema_name(SchemaName("schema")).build()
    assert source_udf_name.name == "udf" \
           and source_udf_name.schema_name is None \
           and udf_name.name == "udf" \
           and udf_name.schema_name.name == "schema" \
           and isinstance(udf_name, UDFName)


def test_from_existing_using_with_name():
    source_udf_name = UDFNameImpl("udf", SchemaName("schema"))
    udf_name = UDFNameBuilder(udf_name=source_udf_name).with_name("udf1").build()
    assert source_udf_name.name == "udf" \
           and source_udf_name.schema_name.name == "schema" \
           and udf_name.schema_name.name == "schema" \
           and udf_name.name == "udf1" \
           and isinstance(udf_name, UDFName)


def test_from_existing_and_new_schema_in_constructor():
    source_udf_name = UDFNameImpl("udf")
    udf_name = UDFNameBuilder(schema=SchemaName("schema"),
                              udf_name=source_udf_name).build()
    assert source_udf_name.name == "udf" \
           and source_udf_name.schema_name is None \
           and udf_name.name == "udf" \
           and udf_name.schema_name.name == "schema" \
           and isinstance(udf_name, UDFName)


def test_from_existing_and_new_name_in_constructor():
    source_udf_name = UDFNameImpl("udf", SchemaName("schema"))
    udf_name = UDFNameBuilder(name="udf1",
                              udf_name=source_udf_name).build()
    assert source_udf_name.name == "udf" \
           and source_udf_name.schema_name.name == "schema" \
           and udf_name.schema_name.name == "schema" \
           and udf_name.name == "udf1" \
           and isinstance(udf_name, UDFName)


def test_using_create_name_using_only_name():
    udf_name = UDFNameBuilder.create(name="udf")
    assert udf_name.name == "udf" \
           and isinstance(udf_name, UDFName) \
           and isinstance(udf_name, UDFName)


def test_using_create_name_using_schema():
    udf_name = UDFNameBuilder.create(name="udf", schema=SchemaName("schema"))
    assert udf_name.name == "udf" \
           and udf_name.schema_name.name == "schema" \
           and isinstance(udf_name, UDFName) \
           and isinstance(udf_name, UDFName)
