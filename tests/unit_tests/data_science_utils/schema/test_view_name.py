import pytest

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl
from exasol_data_science_utils_python.schema.view_name_impl import ViewNameImpl


def test_fully_qualified():
    view = ViewNameImpl("view")
    assert view.fully_qualified == '"view"'


def test_fully_qualified_with_schema():
    view = ViewNameImpl("view", schema=SchemaName("schema"))
    assert view.fully_qualified == '"schema"."view"'


def test_set_new_schema_fail():
    view = ViewNameImpl("abc")
    with pytest.raises(AttributeError) as c:
        view.schema_name = "edf"


def test_equality_true():
    t1 = ViewNameImpl("view", schema=SchemaName("schema"))
    t2 = ViewNameImpl("view", schema=SchemaName("schema"))
    assert t1 == t2


def test_equality_name_not_equal():
    t1 = ViewNameImpl("view1", schema=SchemaName("schema"))
    t2 = ViewNameImpl("view2", schema=SchemaName("schema"))
    assert t1 != t2


def test_equality_schema_not_equal():
    t1 = ViewNameImpl("view", schema=SchemaName("schema1"))
    t2 = ViewNameImpl("view", schema=SchemaName("schema2"))
    assert t1 != t2


def test_not_equal_to_table():
    t1 = ViewNameImpl("view", schema=SchemaName("schema"))
    t2 = TableNameImpl("view", schema=SchemaName("schema"))
    assert t1 != t2
