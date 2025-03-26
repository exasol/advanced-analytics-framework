import logging
import re
import uuid
from enum import (
    Enum,
    auto,
)
from inspect import cleandoc
from unittest.mock import Mock

import pytest

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    CustomQuery,
    DbOperationType,
    ModifyQuery,
    Query,
    SelectQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    DbObjectType,
    SchemaName,
    TableName,
    TableNameImpl,
    decimal_column,
    varchar_column,
)
from tests.utils.audit_table_utils import (
    SAMPLE_LOG_SPAN,
    LogSpan,
    create_insert_query,
)

LOG = logging.getLogger(__name__)


SAMPLE_RUN_ID = uuid.uuid4()


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
        log_span=SAMPLE_LOG_SPAN,
    )
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    assert statement.query_string == cleandoc(
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
          '{SAMPLE_LOG_SPAN.id}',
          'sample log span',
          '{SAMPLE_RUN_ID}'\u0020
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
        log_span=SAMPLE_LOG_SPAN,
    )
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    assert statement.query_string == cleandoc(
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
          '{SAMPLE_LOG_SPAN.id}',
          'sample log span',
          '{SAMPLE_RUN_ID}',
          "SUB_QUERY"."ERROR_MESSAGE"\u0020
        FROM (SELECT ERROR AS ERROR_MESSAGE FROM {other}) as "SUB_QUERY"
        """
    )


def test_modify_query_with_audit_false(audit_table, other_table):
    query = create_insert_query(other_table, audit=False)
    statements = list(audit_table.augment([query]))
    assert len(statements) == 1
    assert statements[0].query_string.startswith(
        f"INSERT INTO {other_table.fully_qualified}"
    )


def test_count_rows(audit_table, other_table):
    query = create_insert_query(
        other_table,
        audit=True,
        parent_log_span=SAMPLE_LOG_SPAN,
    )
    statement = audit_table._count_rows(query, "Phase")
    LOG.debug(f"{statement}")
    otname = other_table.fully_qualified
    assert statement.query_string == cleandoc(
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
          "LOG_SPAN_ID",
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
          '{query.log_span.id}',
          'INSERT',
          '{SAMPLE_LOG_SPAN.id}',
          '{SAMPLE_RUN_ID}'\u0020
        """
    )


def test_modify_query(audit_table, other_table):
    query = create_insert_query(other_table, audit=True)
    statements = list(audit_table.augment([query]))
    for i, stmt in enumerate(statements):
        LOG.debug(f"{i+1}. {stmt.query_string};")

    def expected_query(table_name: TableName) -> Query:
        return ModifyQuery(
            query_string=f"INSERT INTO {table_name.fully_qualified}",
            db_object_type=DbObjectType.TABLE,
            db_object_name=table_name,
            db_operation_type=DbOperationType.INSERT,
        )

    for expected, actual in zip(
        [
            expected_query(audit_table.name),
            expected_query(other_table),
            expected_query(audit_table.name),
        ],
        statements,
    ):
        assert isinstance(actual, ModifyQuery)
        assert actual.query_string.startswith(expected.query_string)
        assert actual.db_object_type == expected.db_object_type
        assert actual.db_object_name == expected.db_object_name
        assert actual.db_operation_type == expected.db_operation_type


def test_unsupported_query_type(audit_table):
    query = Mock(Query, audit=True, query_string="my query string")
    with pytest.raises(TypeError):
        next(audit_table.augment([query]))


class QueryStringCriterion(Enum):
    REGEXP = auto()
    STARTS_WITH = auto()


def assert_queries_match(
    expected: Query,
    actual: Query,
    query_string_criterion: QueryStringCriterion = QueryStringCriterion.REGEXP,
):
    assert isinstance(expected, actual.__class__)
    if query_string_criterion == QueryStringCriterion.STARTS_WITH:
        assert actual.query_string.startswith(expected.query_string)
    else:
        assert re.match(expected.query_string, actual.query_string, re.DOTALL)
    if isinstance(actual, ModifyQuery):
        assert actual.db_object_type == expected.db_object_type
        assert actual.db_object_name == expected.db_object_name
        assert actual.db_operation_type == expected.db_operation_type


def test_query_types(audit_table):
    """
    Construct a list of sample queries together with 1 or multiple regular
    expressions expected to match the resulting queries generated by
    AuditTable.augmented().
    """

    def insert_query(
        table_name: TableName,
        query_string_suffix: str = "",
        audit: bool = False,
    ):
        return create_insert_query(
            table_name,
            audit=audit,
            query_string=(
                f"INSERT INTO {table_name.fully_qualified}{query_string_suffix}"
            ),
        )

    other_table = TableNameImpl("Other", SchemaName("S2"))
    subquery = SelectQueryWithColumnDefinition("select sub query", [])
    samples = [
        [insert_query(other_table, audit=False), insert_query(other_table)],
        [
            insert_query(other_table, audit=True),
            insert_query(audit_table.name, r".* count\(1\)"),
            insert_query(other_table),
            insert_query(audit_table.name, r".* count\(1\)"),
        ],
        [
            AuditQuery(subquery),
            insert_query(audit_table.name, ".* sub query"),
        ],
        [SelectQuery("select query"), SelectQuery("select query")],
        [CustomQuery("custom query"), CustomQuery("custom query")],
    ]
    queries = [s[0] for s in samples]
    statements = list(audit_table.augment(queries))
    expected_matches = []
    for s in samples:
        expected_matches += s[1:]
    for i, (actual, expected) in enumerate(zip(statements, expected_matches)):
        LOG.debug(f"{i+1}. {actual.query_string}")
        assert_queries_match(expected, actual, QueryStringCriterion.REGEXP)
