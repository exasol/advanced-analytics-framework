import logging
from inspect import cleandoc

import pytest

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    SchemaName,
    TableNameImpl,
    decimal_column,
    varchar_column,
)
from tests.utils.audit_table_utils import (
    AuditScenario,
    create_insert_query,
)

LOG = logging.getLogger(__name__)


@pytest.fixture
def audit_scenario():
    db_schema = "S"
    audit_table = AuditTable(db_schema, "A")
    other_table = TableNameImpl("T", SchemaName(db_schema))
    return AuditScenario(audit_table, other_table)


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


def test_x1_audit_query(audit_scenario):
    other = audit_scenario.other_table.fully_qualified
    select = SelectQueryWithColumnDefinition(
        query_string=f"SELECT ERROR AS ERROR_MESSAGE FROM {other}",
        output_columns=[BaseAuditColumns.ERROR_MESSAGE],
    )
    audit_query = AuditQuery(
        select_with_columns=select,
        audit_fields={BaseAuditColumns.EVENT_NAME.name.name: "my event"},
    )
    audit_table = audit_scenario.audit_table
    statement = next(audit_table.augment([audit_query]))
    LOG.debug(f"insert statement: \n{statement}")
    assert statement == cleandoc(
        f"""
        INSERT INTO {audit_table.name.fully_qualified} (
          "EVENT_NAME",
          "LOG_TIMESTAMP",
          "SESSION_ID",
          "ERROR_MESSAGE"
        ) SELECT
          'my event',
          SYSTIMESTAMP(),
          CURRENT_SESSION,
          "SUB_QUERY"."ERROR_MESSAGE"
        FROM VALUES (1) CROSS JOIN
          (SELECT ERROR AS ERROR_MESSAGE FROM {other}) as "SUB_QUERY"
        """
    )


def test_modify_query_with_audit_false():
    table = TableNameImpl("T", SchemaName("S2"))
    query = create_insert_query(table, audit=False)
    statements = list(AuditTable("S1", "A").augment([query]))
    assert len(statements) == 1
    assert statements[0].startswith(f"INSERT INTO {table.fully_qualified}")


def test_count_rows(audit_scenario):
    query = create_insert_query(audit_scenario.other_table, audit=True)
    audit_table = audit_scenario.audit_table
    statement = audit_table._count_rows(query, "Phase")
    LOG.debug(f"{statement}")
    other_table = audit_scenario.other_table.fully_qualified
    assert statement == cleandoc(
        f"""
        INSERT INTO {audit_table.name.fully_qualified} (
          "LOG_TIMESTAMP",
          "SESSION_ID",
          "DB_OBJECT_NAME",
          "DB_OBJECT_SCHEMA",
          "DB_OBJECT_TYPE",
          "EVENT_NAME",
          "LOG_SPAN_NAME",
          "EVENT_ATTRIBUTES",
          "ROW_COUNT"
        ) SELECT
          SYSTIMESTAMP(),
          CURRENT_SESSION,
          'T',
          'S',
          'TABLE',
          'Phase',
          'INSERT',
          '{{"a": 123, "b": "value"}}',
          "SUB_QUERY"."ROW_COUNT"
        FROM VALUES (1)
        CROSS JOIN
          (SELECT count(1) as "ROW_COUNT" FROM {other_table}) as "SUB_QUERY"
        """
    )


def test_modify_query(audit_scenario):
    query = create_insert_query(audit_scenario.other_table, audit=True)
    audit_table = audit_scenario.audit_table
    statements = list(audit_table.augment([query]))
    for i, stmt in enumerate(statements):
        LOG.debug(f"{i+1}. {stmt};")

    other_table = audit_scenario.other_table.fully_qualified
    for expected, actual in zip(
        [
            f"INSERT INTO {audit_table.name.fully_qualified}",
            f"INSERT INTO {other_table}",
            f"INSERT INTO {audit_table.name.fully_qualified}",
        ],
        statements,
    ):
        assert actual.startswith(expected)
