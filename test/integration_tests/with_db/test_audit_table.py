import logging

import pytest

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    LogSpan,
    ModifyQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    SchemaName,
    TableName,
    TableNameImpl,
    decimal_column,
    varchar_column,
)
from test.utils.audit_table_utils import (
    SAMPLE_LOG_SPAN,
    all_rows_as_dicts,
    create_insert_query,
)

LOG = logging.getLogger(__name__)


@pytest.fixture
def audit_table(pyexasol_connection, db_schema):
    table = AuditTable(db_schema, "A")
    for stmt in [
        f"DROP TABLE IF EXISTS {table.name.fully_qualified}",
        table.create_statement,
    ]:
        pyexasol_connection.execute(stmt)
    return table


@pytest.fixture
def subquery_table(pyexasol_connection, db_schema) -> TableName:
    table = TableNameImpl("T", SchemaName(db_schema))
    tname = table.fully_qualified
    for stmt in [
        f"DROP TABLE IF EXISTS {tname}",
        f"CREATE TABLE {table.fully_qualified}"
        ' ("RESULT" DECIMAL, "ERROR" VARCHAR(200))',
    ]:
        pyexasol_connection.execute(stmt)
    return table


def test_create_audit_table(pyexasol_connection, db_schema, exa_all_columns):
    additional_columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    audit_table = AuditTable(db_schema, "pfx", additional_columns)
    pyexasol_connection.execute(audit_table.create_statement)
    actual = exa_all_columns.query(audit_table.name.name)
    expected = {
        c.name.name: c.type.rendered
        for c in (BaseAuditColumns.all + additional_columns)
    }
    assert actual == expected


def test_audit_query(pyexasol_connection, audit_table, subquery_table):
    tname = subquery_table.fully_qualified
    pyexasol_connection.execute(f"INSERT INTO {tname} VALUES (1, 'E1'), (2, 'E2')")
    select = SelectQueryWithColumnDefinition(
        query_string=f"SELECT ERROR AS ERROR_MESSAGE FROM {tname}",
        output_columns=[BaseAuditColumns.ERROR_MESSAGE],
    )
    log_span = LogSpan("LS-1", parent=SAMPLE_LOG_SPAN)
    audit_query = AuditQuery(
        select_with_columns=select,
        audit_fields={BaseAuditColumns.EVENT_NAME.name.name: "my event"},
        log_span=log_span,
    )
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    pyexasol_connection.execute(statement.query_string)
    log_entries = list(all_rows_as_dicts(pyexasol_connection, audit_table.name))
    assert len(log_entries) == 2
    error_messages = [e["ERROR_MESSAGE"] for e in log_entries]
    assert error_messages == ["E1", "E2"]
    for e in log_entries:
        assert e["EVENT_NAME"] == "my event"
        assert e["LOG_SPAN_NAME"] == log_span.name
        assert e["LOG_SPAN_ID"] == log_span.id.hex
        assert e["PARENT_LOG_SPAN_ID"] == log_span.parent.id.hex


def test_modify_query(pyexasol_connection, audit_table, subquery_table):
    def all_rows(table: TableName):
        return list(all_rows_as_dicts(pyexasol_connection, table))

    tname = subquery_table.fully_qualified
    query = create_insert_query(
        subquery_table,
        audit=True,
        parent_log_span=SAMPLE_LOG_SPAN,
    )
    statements = list(audit_table.augment([query]))
    for i, stmt in enumerate(statements):
        LOG.debug(f"{i+1}. {stmt};")
        pyexasol_connection.execute(stmt.query_string)

    log_entries = all_rows(audit_table.name)
    assert len(log_entries) == 2

    properties = [(e["EVENT_NAME"], e["ROW_COUNT"]) for e in log_entries]
    assert properties == [("Begin", "0"), ("End", "2")]

    expected = {
        "DB_OBJECT_SCHEMA": subquery_table.schema_name.name,
        "DB_OBJECT_NAME": subquery_table.name,
        "DB_OBJECT_TYPE": "TABLE",
        "LOG_SPAN_NAME": "INSERT",
        "PARENT_LOG_SPAN_ID": SAMPLE_LOG_SPAN.id.hex,
        "EVENT_ATTRIBUTES": '{"a": 123, "b": "value"}',
    }
    for e in log_entries:
        actual = {k: e[k] for k in expected}
        assert actual == expected
    other_table_rows = all_rows(subquery_table)
    assert len(other_table_rows) == 2
