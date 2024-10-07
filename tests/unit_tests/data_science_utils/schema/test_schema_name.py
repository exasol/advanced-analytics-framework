from exasol_data_science_utils_python.schema.schema_name import SchemaName


def test_fully_qualified():
    schema = SchemaName("schema")
    assert schema.fully_qualified == '"schema"'


def test_equality():
    schema1 = SchemaName("schema")
    schema2 = SchemaName("schema")
    assert schema1 == schema2


def test_inequality():
    schema1 = SchemaName("schema1")
    schema2 = SchemaName("schema2")
    assert schema1 != schema2


def test_hash_equality():
    schema1 = SchemaName("schema")
    schema2 = SchemaName("schema")
    assert hash(schema1) == hash(schema2)


def test_hash_inequality():
    schema1 = SchemaName("schema1")
    schema2 = SchemaName("schema2")
    assert hash(schema1) != hash(schema2)
