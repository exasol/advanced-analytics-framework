import contextlib
from typing import (
    Callable,
    Union,
)
from unittest.mock import (
    Mock,
    patch,
)

import pytest

from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.audit_query_handler import (
    AuditQueryHandler,
    ParameterType,
    ResultType,
)
from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    TopLevelQueryHandlerContext,
)
from exasol.analytics.query_handler.python_query_handler_runner import (
    PythonQueryHandlerRunner,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query.select import AuditQuery
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)
from exasol.analytics.schema import (
    Column,
    SchemaName,
    TableName,
    TableNameImpl,
    ViewName,
    ViewNameImpl,
    decimal_column,
)
from exasol.analytics.sql_executor.testing.mock_result_set import MockResultSet
from exasol.analytics.sql_executor.testing.mock_sql_executor import (
    MockSQLExecutor,
    create_sql_executor,
    expect_query,
)
from tests.unit_tests.audit_log.qh_utils import continue_action
from tests.utils.audit_table_utils import create_insert_query


@pytest.fixture()
def prefix(tmp_db_obj_prefix):
    return tmp_db_obj_prefix


@pytest.fixture
def context_mock(top_level_query_handler_context_mock) -> TopLevelQueryHandlerContext:
    return top_level_query_handler_context_mock


class SamplePayloadQueryHandler(QueryHandler[str, str]):
    def __init__(self, parameter: str, context: ScopeQueryHandlerContext):
        super().__init__(parameter, context)
        self._parameter = parameter

    def start(self) -> Continue | Finish:
        query = create_insert_query(TableNameImpl("T", SchemaName("S2")), audit=True)
        return continue_action([query])

    def handle_query_result(self, result: QueryResult) -> Continue | Finish:
        event_attributes = '{"c": 456}'
        audit_query = AuditQuery(audit_fields={"EVENT_ATTRIBUTES": event_attributes})
        return Finish(self._parameter, audit_query=audit_query)


@contextlib.contextmanager
def audit_query_handler_factory(
    db_schema: str,
    payload_qh_factory: Callable[
        [ParameterType, ScopeQueryHandlerContext],
        QueryHandler[ParameterType, ResultType],
    ],
    table_name_prefix: str = "P",
    additional_columns: list[Column] = [],
):
    def factory(
        parameter: ParameterType,
        context: ScopeQueryHandlerContext,
    ) -> QueryHandler[ParameterType, ResultType]:
        def schema_getter(parameter: ParameterType) -> str:
            return db_schema

        def table_name_prefix_getter(parameter: ParameterType) -> str:
            return table_name_prefix

        return AuditQueryHandler(
            parameter=parameter,
            context=context,
            query_handler_factory=payload_qh_factory,
            schema_getter=schema_getter,
            table_name_prefix_getter=table_name_prefix_getter,
            additional_columns=additional_columns,
        )

    yield factory


def uuid_generator(id: str):
    i = 0
    while True:
        i += 1
        yield f"UUID-{id}-{i}"


def expect_count_rows(
    table_name: TableName,
    query: str,
) -> list[list[str, MockResultSet]]:
    def count_rows(event_name: str) -> list[str, MockResultSet]:
        return expect_query(
            f"""
            INSERT INTO {table_name.fully_qualified} (
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
              "RUN_ID"
            ) SELECT
              SYSTIMESTAMP(),
              (SELECT count(1) FROM "S2"."T"),
              CURRENT_SESSION,
              'T',
              'S2',
              'TABLE',
              '{{{{"a": 123, "b": "value"}}}}',
              '{event_name}',
              'UUID-2-2',
              'INSERT',
              'UUID-2-1'\u0020
            """
        )

    return [
        count_rows("Begin"),
        expect_query(query),
        count_rows("End"),
    ]


def expect_query_with_temp_view(
    db_schema: str,
    view_name: str,
    query: str,
    column: Column,
) -> list[list[str, MockResultSet]]:
    view_fq = ViewNameImpl(view_name, SchemaName(db_schema)).fully_qualified
    return [
        expect_query(
            f"""
            CREATE OR REPLACE VIEW {view_fq} AS
            {query} as {column.name.name};
            """
        ),
        expect_query(
            f"""
            SELECT
                "{column.name.name}"
            FROM {view_fq};
            """,
            MockResultSet(rows=[(1,)], columns=[column]),
        ),
        expect_query(f"DROP VIEW IF EXISTS {view_fq};"),
    ]


@patch("exasol.analytics.audit.audit.uuid.uuid4")
@patch("exasol.analytics.query_handler.query.select.uuid.uuid4")
def test_audit(uuid_mock_1, uuid_mock_2, aaf_pytest_db_schema, prefix, context_mock):
    uuid_mock_1.side_effect = uuid_generator("1")
    uuid_mock_2.side_effect = uuid_generator("2")
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(""),
        expect_query(""),
    )
    audit_table = AuditTable(aaf_pytest_db_schema, prefix, run_id=Mock())
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        # create audit table
        expect_query(audit_table.create_statement),
        # modify query incl. counting rows before and after
        *expect_count_rows(
            audit_table.name,
            """
            INSERT INTO "S2"."T" ("RESULT", "ERROR") VALUES (3, 'E3'), (4, 'E4')
            """,
        ),
        # continue input query
        *expect_query_with_temp_view(
            aaf_pytest_db_schema,
            f"{prefix}_4_1",
            "SELECT 1",
            decimal_column("CONTINUE_INPUT_COLUMN", precision=1, scale=0),
        ),
        # final audit log query
        expect_query(
            f"""
            INSERT INTO {audit_table.name.fully_qualified} (
              "LOG_TIMESTAMP",
              "SESSION_ID",
              "EVENT_ATTRIBUTES"
            ) SELECT
              SYSTIMESTAMP(),
              CURRENT_SESSION,
              '{{{{"c": 456}}}}'\u0020
            """
        ),
        # sub query of final audit log query
        *expect_query_with_temp_view(
            aaf_pytest_db_schema,
            f"{prefix}_6_1",
            "SELECT (CAST 1 as DECIMAL(1,0))",
            decimal_column("DUMMY_COLUMN", precision=1, scale=0),
        ),
    )
    sample = "hello world"
    payload_qh_factory = SamplePayloadQueryHandler
    with audit_query_handler_factory(
        aaf_pytest_db_schema,
        payload_qh_factory=payload_qh_factory,
        table_name_prefix=prefix,
    ) as factory:
        runner = PythonQueryHandlerRunner[str, str](
            sql_executor=sql_executor,
            top_level_query_handler_context=context_mock,
            parameter=sample,
            query_handler_factory=factory,
        )
        output = runner.run()
    assert output == sample
