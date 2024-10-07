import pytest

from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.udf_name_impl import UDFNameImpl


def test_fully_qualified():
    udf = UDFNameImpl("udf")
    assert udf.fully_qualified == '"udf"'


def test_fully_qualified_with_schema():
    udf = UDFNameImpl("udf", schema=SchemaName("schema"))
    assert udf.fully_qualified == '"schema"."udf"'


def test_set_new_schema_fail():
    udf = UDFNameImpl("abc")
    with pytest.raises(AttributeError) as c:
        udf.schema_name = "edf"


def test_equality_true():
    t1 = UDFNameImpl("udf", schema=SchemaName("schema"))
    t2 = UDFNameImpl("udf", schema=SchemaName("schema"))
    assert t1 == t2


def test_equality_name_not_equal():
    t1 = UDFNameImpl("udf1", schema=SchemaName("schema"))
    t2 = UDFNameImpl("udf2", schema=SchemaName("schema"))
    assert t1 != t2


def test_equality_schema_not_equal():
    t1 = UDFNameImpl("udf", schema=SchemaName("schema1"))
    t2 = UDFNameImpl("udf", schema=SchemaName("schema2"))
    assert t1 != t2


def test_hash_equality_true():
    t1 = UDFNameImpl("udf", schema=SchemaName("schema"))
    t2 = UDFNameImpl("udf", schema=SchemaName("schema"))
    assert hash(t1) == hash(t2)


def test_hash_equality_name_not_equal():
    t1 = UDFNameImpl("udf1", schema=SchemaName("schema"))
    t2 = UDFNameImpl("udf2", schema=SchemaName("schema"))
    assert hash(t1) != hash(t2)


def test_hash_equality_schema_not_equal():
    t1 = UDFNameImpl("udf", schema=SchemaName("schema1"))
    t2 = UDFNameImpl("udf", schema=SchemaName("schema2"))
    assert hash(t1) != hash(t2)
