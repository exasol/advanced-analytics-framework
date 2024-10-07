from exasol_data_science_utils_python.schema.connection_object_name_impl import ConnectionObjectNameImpl

CONNECTION_UPPER = "CONNECTION"

CONNECTION = "connection"


def test_constructor():
    connection_object_name = ConnectionObjectNameImpl(name=CONNECTION)
    assert connection_object_name.name == CONNECTION


def test_quoted_name():
    connection_object_name = ConnectionObjectNameImpl(name=CONNECTION)
    assert connection_object_name.quoted_name == f'"{CONNECTION}"'


def test_fully_qualified():
    connection_object_name = ConnectionObjectNameImpl(name=CONNECTION)
    assert connection_object_name.fully_qualified == f'"{CONNECTION}"'


def test_normalized_name_for_udfs():
    connection_object_name = ConnectionObjectNameImpl(name=CONNECTION)
    assert connection_object_name.normalized_name_for_udfs == CONNECTION_UPPER


def test_equality():
    connection_object_name1 = ConnectionObjectNameImpl(name=CONNECTION)
    connection_object_name2 = ConnectionObjectNameImpl(name=CONNECTION)
    assert connection_object_name2 == connection_object_name1


def test_equality_case_insensitive():
    connection_object_name1 = ConnectionObjectNameImpl(name=CONNECTION)
    connection_object_name2 = ConnectionObjectNameImpl(name=CONNECTION_UPPER)
    assert connection_object_name2 == connection_object_name1


def test_inequality():
    connection_object_name1 = ConnectionObjectNameImpl(name="con1")
    connection_object_name2 = ConnectionObjectNameImpl(name="con2")
    assert connection_object_name2 != connection_object_name1


def test_hash_equality():
    connection_object_name1 = ConnectionObjectNameImpl(name=CONNECTION)
    connection_object_name2 = ConnectionObjectNameImpl(name=CONNECTION)
    assert hash(connection_object_name2) == hash(connection_object_name1)


def test_hash_equality_case_insensitive():
    connection_object_name1 = ConnectionObjectNameImpl(name=CONNECTION)
    connection_object_name2 = ConnectionObjectNameImpl(name=CONNECTION_UPPER)
    assert hash(connection_object_name2) == hash(connection_object_name1)


def test_hash_inequality():
    connection_object_name1 = ConnectionObjectNameImpl(name="con1")
    connection_object_name2 = ConnectionObjectNameImpl(name="con2")
    assert hash(connection_object_name2) != hash(connection_object_name1)
