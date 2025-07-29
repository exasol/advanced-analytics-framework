from test.utils.audit_table_utils import (
    SAMPLE_LOG_SPAN,
    create_insert_query,
)
from unittest.mock import Mock

import pytest

from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    CustomQuery,
    LogSpan,
    ModifyQuery,
    SelectQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    DbObjectType,
    DbOperationType,
    SchemaName,
    TableNameImpl,
)


def modify_query(audit: bool):
    return create_insert_query(TableNameImpl("hello"), audit)


@pytest.mark.parametrize(
    "query, expected",
    [
        (CustomQuery(""), False),
        (SelectQuery(""), False),
        (AuditQuery(), True),
        (modify_query(audit=True), True),
        (modify_query(audit=False), False),
    ],
)
def test_audit_property(query, expected):
    assert query.audit == expected


def test_audit_query():
    query = "my_query"
    ad_query = AuditQuery(SelectQueryWithColumnDefinition(query, []))
    assert ad_query.query_string == query


def test_modify_query():
    query = "my_query"
    db_object_name = TableNameImpl("a_table", SchemaName("a_schema"))
    audit_fields = {"col_1": "value", "col_2": 123}
    testee = ModifyQuery(
        query,
        db_object_type=DbObjectType.TABLE,
        db_object_name=db_object_name,
        db_operation_type=DbOperationType.CREATE,
        audit_fields=audit_fields,
    )
    assert testee.query_string == query
    assert testee.db_object_name == db_object_name
    assert testee.db_object_type == DbObjectType.TABLE
    assert testee.db_operation_type == DbOperationType.CREATE
    assert testee.audit_fields == audit_fields


@pytest.mark.parametrize(
    "db_object_type, db_operation_type, expected_modifies",
    [
        (DbObjectType.TABLE, DbOperationType.INSERT, True),
        (DbObjectType.TABLE, DbOperationType.CREATE, True),
        (DbObjectType.TABLE, DbOperationType.CREATE_OR_REPLACE, True),
        (DbObjectType.TABLE, DbOperationType.CREATE_IF_NOT_EXISTS, True),
        (DbObjectType.TABLE, DbOperationType.UPDATE, False),
        (DbObjectType.SCHEMA, DbOperationType.INSERT, False),
    ],
)
def test_query_modifies_row_count(
    db_object_type: DbObjectType,
    db_operation_type: DbOperationType,
    expected_modifies,
):
    query = ModifyQuery(
        "SELECT 1",
        db_object_name=TableNameImpl("a_table"),
        db_object_type=db_object_type,
        db_operation_type=db_operation_type,
    )
    assert query.modifies_row_count == expected_modifies


@pytest.mark.parametrize("db_operation_type", DbOperationType)
def test_log_span_modify_query(db_operation_type: DbOperationType):
    actual = ModifyQuery(
        query_string="query_string",
        db_object_name=TableNameImpl("tbl"),
        db_object_type=DbObjectType.TABLE,
        db_operation_type=db_operation_type,
        parent_log_span=SAMPLE_LOG_SPAN,
    ).log_span
    assert actual.name == db_operation_type.name
    assert actual.parent == SAMPLE_LOG_SPAN


def test_log_span_audit_query():
    assert AuditQuery(log_span=SAMPLE_LOG_SPAN).log_span == SAMPLE_LOG_SPAN
