import pytest

from exasol.analytics.schema import (
    SchemaName,
    ViewNameBuilder,
    ViewNameImpl,
    ViewName,
)
from typeguard import TypeCheckError


def test_using_empty_constructor():
    with pytest.raises(TypeCheckError):
        view_name = ViewNameBuilder().build()


def test_using_constructor_name_only():
    view_name = ViewNameBuilder(name="view").build()
    assert view_name.name == "view" \
           and view_name.schema_name is None \
           and isinstance(view_name, ViewName)


def test_using_constructor_schema():
    view_name = ViewNameBuilder(name="view", schema=SchemaName("schema")).build()
    assert view_name.name == "view" \
           and view_name.schema_name.name is "schema" \
           and isinstance(view_name, ViewName)


def test_using_with_name_only():
    view_name = ViewNameBuilder().with_name("view").build()
    assert view_name.name == "view" \
           and view_name.schema_name is None \
           and isinstance(view_name, ViewName)


def test_using_with_schema():
    view_name = ViewNameBuilder().with_name("view").with_schema_name(SchemaName("schema")).build()
    assert view_name.name == "view" \
           and view_name.schema_name.name == "schema" \
           and isinstance(view_name, ViewName)


def test_from_existing_using_with_schema():
    source_view_name = ViewNameImpl("view")
    view_name = ViewNameBuilder(view_name=source_view_name).with_schema_name(SchemaName("schema")).build()
    assert source_view_name.name == "view" \
           and source_view_name.schema_name is None \
           and view_name.name == "view" \
           and view_name.schema_name.name == "schema" \
           and isinstance(view_name, ViewName)


def test_from_existing_using_with_name():
    source_view_name = ViewNameImpl("view", SchemaName("schema"))
    view_name = ViewNameBuilder(view_name=source_view_name).with_name("view1").build()
    assert source_view_name.name == "view" \
           and source_view_name.schema_name.name == "schema" \
           and view_name.schema_name.name == "schema" \
           and view_name.name == "view1" \
           and isinstance(view_name, ViewName)


def test_from_existing_and_new_schema_in_constructor():
    source_view_name = ViewNameImpl("view")
    view_name = ViewNameBuilder(schema=SchemaName("schema"),
                                view_name=source_view_name).build()
    assert source_view_name.name == "view" \
           and source_view_name.schema_name is None \
           and view_name.name == "view" \
           and view_name.schema_name.name == "schema" \
           and isinstance(view_name, ViewName)


def test_from_existing_and_new_name_in_constructor():
    source_view_name = ViewNameImpl("view", SchemaName("schema"))
    view_name = ViewNameBuilder(name="view1",
                                view_name=source_view_name).build()
    assert source_view_name.name == "view" \
           and source_view_name.schema_name.name == "schema" \
           and view_name.schema_name.name == "schema" \
           and view_name.name == "view1" \
           and isinstance(view_name, ViewName)


def test_using_create_name_using_only_name():
    view_name = ViewNameBuilder.create(name="view")
    assert view_name.name == "view" \
           and isinstance(view_name, ViewName)


def test_using_create_name_using_schema():
    view_name = ViewNameBuilder.create(name="view", schema=SchemaName("schema"))
    assert view_name.name == "view" \
           and view_name.schema_name.name == "schema" \
           and isinstance(view_name, ViewName)
