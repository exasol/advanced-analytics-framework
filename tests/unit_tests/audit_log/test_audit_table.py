import logging
import re
import uuid
from inspect import cleandoc
from unittest.mock import Mock

import pytest

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    CustomQuery,
    Query,
    SelectQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    SchemaName,
    TableNameImpl,
    decimal_column,
    varchar_column,
)
from tests.utils.audit_table_utils import (
    LogSpan,
    SAMPLE_UUID,
    create_insert_query,
)

LOG = logging.getLogger(__name__)


SAMPLE_RUN_ID = uuid.uuid4().hex



@pytest.fixture
def audit_table():
    return AuditTable("S1", "A", run_id=SAMPLE_RUN_ID)


@pytest.fixture
def other_table():
    return TableNameImpl("T", SchemaName("S2"))


def test_init():
    additional_columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    audit_table = AuditTable("my_schema", "pfx", additional_columns)
    assert audit_table.name.schema_name.name == "my_schema"
    assert audit_table.name.name.startswith("pfx_")
    assert set(additional_columns) <= set(audit_table.columns)


def test_empty_prefix():
    with pytest.raises(ValueError):
        AuditTable("my_schema", "")


def test_audit_query_no_subquery(audit_table, other_table):
    other = other_table.fully_qualified
    audit_query = AuditQuery(
        audit_fields={BaseAuditColumns.EVENT_NAME.name.name: "my event"},
        log_span=LogSpan("log span", SAMPLE_UUID),
    )
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    assert statement == cleandoc(
        f"""
        INSERT INTO {audit_table.name.fully_qualified} (
          "LOG_TIMESTAMP",
          "SESSION_ID",
          "EVENT_NAME",
          "LOG_SPAN_ID",
          "LOG_SPAN_NAME",
          "RUN_ID"
        ) SELECT
          SYSTIMESTAMP(),
          CURRENT_SESSION,
          'my event',
          '{SAMPLE_UUID.hex}',
          'log span',
          '{SAMPLE_RUN_ID}'
        """
    )


def test_audit_query_with_subquery(audit_table, other_table):
    other = other_table.fully_qualified
    select = SelectQueryWithColumnDefinition(
        query_string=f"SELECT ERROR AS ERROR_MESSAGE FROM {other}",
        output_columns=[BaseAuditColumns.ERROR_MESSAGE],
    )
    audit_query = AuditQuery(
        select_with_columns=select,
        audit_fields={BaseAuditColumns.EVENT_NAME.name.name: "my event"},
        log_span=LogSpan("log span", SAMPLE_UUID),
    )
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    assert statement == cleandoc(
        f"""
        INSERT INTO {audit_table.name.fully_qualified} (
          "LOG_TIMESTAMP",
          "SESSION_ID",
          "EVENT_NAME",
          "LOG_SPAN_ID",
          "LOG_SPAN_NAME",
          "RUN_ID",
          "ERROR_MESSAGE"
        ) SELECT
          SYSTIMESTAMP(),
          CURRENT_SESSION,
          'my event',
          '{SAMPLE_UUID.hex}',
          'log span',
          '{SAMPLE_RUN_ID}',
          "SUB_QUERY"."ERROR_MESSAGE"
        FROM (SELECT ERROR AS ERROR_MESSAGE FROM {other}) as "SUB_QUERY"
        """
    )


def test_modify_query_with_audit_false(audit_table, other_table):
    query = create_insert_query(other_table, audit=False)
    statements = list(audit_table.augment([query]))
    assert len(statements) == 1
    assert statements[0].startswith(f"INSERT INTO {other_table.fully_qualified}")


def test_count_rows(audit_table, other_table):
    query = create_insert_query(other_table, audit=True)
    statement = audit_table._count_rows(query, "Phase")
    LOG.debug(f"{statement}")
    otname = other_table.fully_qualified
    assert statement == cleandoc(
        f"""
        INSERT INTO {audit_table.name.fully_qualified} (
          "LOG_TIMESTAMP",
          "ROW_COUNT",
          "SESSION_ID",
          "DB_OBJECT_NAME",
          "DB_OBJECT_SCHEMA",
          "DB_OBJECT_TYPE",
          "EVENT_ATTRIBUTES",
          "EVENT_NAME",
          "LOG_SPAN_NAME",
          "PARENT_LOG_SPAN_ID",
          "RUN_ID"
        ) SELECT
          SYSTIMESTAMP(),
          (SELECT count(1) FROM {otname}),
          CURRENT_SESSION,
          '{other_table.name}',
          '{other_table.schema_name.name}',
          'TABLE',
          '{{"a": 123, "b": "value"}}',
          'Phase',
          'INSERT',
          '{SAMPLE_UUID.hex}',
          '{SAMPLE_RUN_ID}'
        """
    )


def test_modify_query(audit_table, other_table):
    query = create_insert_query(other_table, audit=True)
    statements = list(audit_table.augment([query]))
    for i, stmt in enumerate(statements):
        LOG.debug(f"{i+1}. {stmt};")

    other_table = other_table.fully_qualified
    for expected, actual in zip(
        [
            f"INSERT INTO {audit_table.name.fully_qualified}",
            f"INSERT INTO {other_table}",
            f"INSERT INTO {audit_table.name.fully_qualified}",
        ],
        statements,
    ):
        assert actual.startswith(expected)


def test_unsupported_query_type(audit_table):
    query = Mock(Query, audit=True, query_string="my query string")
    with pytest.raises(TypeError):
        next(audit_table.augment([query]))


def test_query_types(audit_table):
    """
    Construct a list of sample queries together with 1 or multiple regular
    expressions expected to match the resulting queries generated by
    AuditTable.augmented().
    """

    def insert_query(query_string: str, audit: bool):
        return create_insert_query(
            TableNameImpl("table"),
            audit=audit,
            query_string=query_string,
        )

    subquery = SelectQueryWithColumnDefinition("select sub query", [])
    samples = [
        [insert_query("insert query 1", audit=False), "insert query 1"],
        [
            insert_query("insert query 2", audit=True),
            r'INSERT INTO "S1"."A_AUDIT_LOG".* count\(1\)',
            r"insert query 2",
            r'INSERT INTO "S1"."A_AUDIT_LOG".* count\(1\)',
        ],
        [
            AuditQuery(subquery),
            r'INSERT INTO "S1"."A_AUDIT_LOG".* sub query',
        ],
        [SelectQuery("select query"), "select query"],
        [CustomQuery("custom query"), "custom query"],
    ]
    queries = [s[0] for s in samples]
    statements = list(audit_table.augment(queries))
    expected_matches = []
    for s in samples:
        expected_matches += s[1:]
    for i, (actual, expected) in enumerate(zip(statements, expected_matches)):
        matches = re.match(expected, actual, re.DOTALL)
        LOG.debug(f"{i+1}. {matches and True}")
        assert matches
