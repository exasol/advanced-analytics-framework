import pytest

from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    ModifyQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    DbObjectType,
    DbOperationType,
    TableLikeNameImpl,
)


def test_audit_query():
    query = "my_query"
    ad_query = AuditQuery(SelectQueryWithColumnDefinition(query, []))
    assert ad_query.query_string == query


def test_modify_query():
    query = "my_query"
    table_name = (TableLikeNameImpl("table_name"),)
    mod_query = ModifyQuery(
        query,
        db_object_name=table_name,
        db_object_type=DbObjectType.TABLE,
        db_operation_type=DbOperationType.CREATE,
    )
    assert mod_query.query_string == query
    assert mod_query.db_object_name == table_name
    assert mod_query.db_object_type == "TABLE"
    assert mod_query.db_operation_type == "CREATE"


@pytest.mark.parametrize(
    "db_object_type, db_operation_type, expected_modifies",
    [
        ("TABLE", "INSERT", True),
        ("TABLE", "CREATE", True),
        ("TABLE", "CREATE_OR_REPLACE", True),
        ("TABLE", "CREATE_IF_NOT_EXISTS", True),
        ("TABLE", "UPDATE", False),
        ("SCHEMA", "INSERT", False),
    ],
)
def test_query_modifies_row_count(
    db_object_type,
    db_operation_type,
    expected_modifies,
):
    query = ModifyQuery(
        "SELECT 1",
        db_object_name=TableLikeNameImpl("table_name"),
        db_object_type=db_object_type,
        db_operation_type=db_operation_type,
    )
    assert query.modifies_row_count == expected_modifies
