import pytest

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl


def test_fully_qualified():
    table = TableNameImpl("table")
    assert table.fully_qualified == '"table"'


def test_fully_qualified_with_schema():
    table = TableNameImpl("table", schema=SchemaName("schema"))
    assert table.fully_qualified == '"schema"."table"'


def test_set_new_schema_fail():
    table = TableNameImpl("abc")
    with pytest.raises(AttributeError) as c:
        table.schema_name = "edf"


def test_equality_true():
    t1 = TableNameImpl("table", schema=SchemaName("schema"))
    t2 = TableNameImpl("table", schema=SchemaName("schema"))
    assert t1 == t2


def test_equality_name_not_equal():
    t1 = TableNameImpl("table1", schema=SchemaName("schema"))
    t2 = TableNameImpl("table2", schema=SchemaName("schema"))
    assert t1 != t2


def test_equality_schema_not_equal():
    t1 = TableNameImpl("table", schema=SchemaName("schema1"))
    t2 = TableNameImpl("table", schema=SchemaName("schema2"))
    assert t1 != t2
