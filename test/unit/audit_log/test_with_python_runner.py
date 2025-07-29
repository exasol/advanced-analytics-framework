from typing import Callable
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
from test.unit.audit_log.qh_utils import continue_action
from test.utils.audit_table_utils import create_insert_query


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
        self._counter = 0

    def start(self) -> Continue | Finish:
        query = create_insert_query(TableNameImpl("T1", SchemaName("S1")), audit=True)
        return continue_action([query])

    def handle_query_result(self, result: QueryResult) -> Continue | Finish:
        if self._counter < 1:
            self._counter += 1
            query = create_insert_query(
                TableNameImpl("T2", SchemaName("S2")), audit=True
            )
            return continue_action([query])
        else:
            event_attributes = '{"c": 456}'
            audit_query = AuditQuery(
                audit_fields={"EVENT_ATTRIBUTES": event_attributes}
            )
            return Finish(self._parameter, audit_query=audit_query)


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

    return factory


def expect_count_rows(
    audit_table: TableName,
    other_table: TableName,
    log_span_id: str,
    query: str,
) -> list[list[str, MockResultSet]]:
    def count_rows(event_name: str) -> list[str, MockResultSet]:
        return expect_query(
            f"""
            INSERT INTO {audit_table.fully_qualified} (
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
              (SELECT count(1) FROM {other_table.fully_qualified}),
              CURRENT_SESSION,
              '{other_table.name}',
              '{other_table.schema_name.name}',
              'TABLE',
              '{{{{"a": 123, "b": "value"}}}}',
              '{event_name}',
              '{log_span_id}',
              'INSERT',
              'RUN_ID_2'
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


def id_generator(prefix: str):
    i = 0
    while True:
        i += 1
        yield f"{prefix}_{i}"


@patch("exasol.analytics.audit.audit._generate_run_id")
@patch("exasol.analytics.query_handler.query.select._generate_log_span_id")
def test_audit(
    log_span_id_mock, run_id_mock, aaf_pytest_db_schema, prefix, context_mock
):
    log_span_id_mock.side_effect = id_generator("LOG_SPAN")
    run_id_mock.side_effect = id_generator("RUN_ID")
    audit_table_name_prefix = "AP"
    audit_table = AuditTable(
        aaf_pytest_db_schema,
        audit_table_name_prefix,
    )
    other_table_1 = TableNameImpl("T1", SchemaName("S1"))
    other_table_2 = TableNameImpl("T2", SchemaName("S2"))
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        # create audit table
        expect_query(audit_table.create_statement),
        # modify query #1 incl. counting rows before and after
        *expect_count_rows(
            audit_table.name,
            other_table_1,
            "LOG_SPAN_1",
            (
                f"INSERT INTO {other_table_1.fully_qualified}"
                """ ("RESULT", "ERROR") VALUES (3, 'E3'), (4, 'E4')"""
            ),
        ),
        # continue input query
        *expect_query_with_temp_view(
            aaf_pytest_db_schema,
            f"{prefix}_4_1",
            "SELECT 1",
            decimal_column("CONTINUE_INPUT_COLUMN", precision=1, scale=0),
        ),
        # modify query #2
        *expect_count_rows(
            audit_table.name,
            other_table_2,
            "LOG_SPAN_5",
            (
                f"INSERT INTO {other_table_2.fully_qualified}"
                """ ("RESULT", "ERROR") VALUES (3, 'E3'), (4, 'E4')"""
            ),
        ),
        # continue input query
        *expect_query_with_temp_view(
            aaf_pytest_db_schema,
            f"{prefix}_6_1",
            "SELECT 1",
            decimal_column("CONTINUE_INPUT_COLUMN", precision=1, scale=0),
        ),
        # final audit log query
        expect_query(
            f"""
            INSERT INTO {audit_table.name.fully_qualified} (
              "LOG_TIMESTAMP",
              "SESSION_ID",
              "EVENT_ATTRIBUTES",
              "RUN_ID"
            ) SELECT
              SYSTIMESTAMP(),
              CURRENT_SESSION,
              '{{{{"c": 456}}}}',
              'RUN_ID_2'
            """
        ),
        # sub query of final audit log query
        *expect_query_with_temp_view(
            aaf_pytest_db_schema,
            f"{prefix}_8_1",
            "SELECT (CAST 1 as DECIMAL(1,0))",
            decimal_column("DUMMY_COLUMN", precision=1, scale=0),
        ),
    )
    sample = "hello world"
    payload_qh_factory = SamplePayloadQueryHandler
    factory = audit_query_handler_factory(
        aaf_pytest_db_schema,
        payload_qh_factory=payload_qh_factory,
        table_name_prefix=audit_table_name_prefix,
    )
    runner = PythonQueryHandlerRunner[str, str](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=sample,
        query_handler_factory=factory,
    )
    output = runner.run()
    assert output == sample
