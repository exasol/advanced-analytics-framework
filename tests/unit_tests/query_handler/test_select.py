import pytest

from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    ModifyQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    DbObjectType,
    DbSpanType,
    SchemaName,
    TableLikeNameImpl,
)


def test_audit_query():
    query = "my_query"
    ad_query = AuditQuery(SelectQueryWithColumnDefinition(query, []))
    assert ad_query.query_string == query


def test_modify_query():
    query = "my_query"
    db_object_name = TableLikeNameImpl("a_table", SchemaName("a_schema"))
    audit_fields = {"col_1": "value", "col_2": 123}
    testee = ModifyQuery(
        query,
        db_object_type=DbObjectType.TABLE,
        db_object_name=db_object_name,
        db_span_type=DbSpanType.CREATE,
        audit_fields=audit_fields,
    )
    assert testee.query_string == query
    assert testee.db_object_name == db_object_name
    assert testee.db_object_type == DbObjectType.TABLE
    assert testee.db_span_type == DbSpanType.CREATE
    assert testee.audit_fields == audit_fields


@pytest.mark.parametrize(
    "db_object_type, db_span_type, expected_modifies",
    [
        (DbObjectType.TABLE, DbSpanType.INSERT, True),
        (DbObjectType.TABLE, DbSpanType.CREATE, True),
        (DbObjectType.TABLE, DbSpanType.CREATE_OR_REPLACE, True),
        (DbObjectType.TABLE, DbSpanType.CREATE_IF_NOT_EXISTS, True),
        (DbObjectType.TABLE, DbSpanType.UPDATE, False),
        (DbObjectType.SCHEMA, DbSpanType.INSERT, False),
    ],
)
def test_query_modifies_row_count(
    db_object_type: DbObjectType,
    db_span_type: DbSpanType,
    expected_modifies,
):
    query = ModifyQuery(
        "SELECT 1",
        db_object_name=TableLikeNameImpl("a_table"),
        db_object_type=db_object_type,
        db_span_type=db_span_type,
    )
    assert query.modifies_row_count == expected_modifies
