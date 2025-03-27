import contextlib
from typing import (
    Callable,
    Union,
)
from unittest.mock import Mock

import pytest

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
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)
from exasol.analytics.schema import Column
from exasol.analytics.sql_executor.testing.mock_sql_executor import MockSQLExecutor


@pytest.fixture
def context_mock(top_level_query_handler_context_mock) -> TopLevelQueryHandlerContext:
    return top_level_query_handler_context_mock


class SampleInput:
    pass


class SampleOutput:
    def __init__(self, result: SampleInput):
        self.result = result


class StartFinishQh(QueryHandler[SampleInput, SampleOutput]):
    def __init__(
        self,
        parameter: SampleInput,
        context: ScopeQueryHandlerContext,
    ):
        super().__init__(parameter, context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[SampleOutput]]:
        return Finish[SampleOutput](SampleOutput(self._parameter))

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[SampleOutput]]:
        pass


@contextlib.contextmanager
def audit_query_handler_factory(
    db_schema: str,
    child_qh_factory: Callable[
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
            query_handler_factory=child_qh_factory,
            schema_getter=schema_getter,
            table_name_prefix_getter=table_name_prefix_getter,
            additional_columns=additional_columns,
        )
    yield factory


def test_audit(aaf_pytest_db_schema, context_mock):
    sql_executor = MockSQLExecutor()
    sample = SampleInput()
    child_qh_factory=StartFinishQh
    with audit_query_handler_factory(
        aaf_pytest_db_schema,
        child_qh_factory=child_qh_factory,
    ) as factory:
        runner = PythonQueryHandlerRunner[SampleInput, SampleOutput](
            sql_executor=sql_executor,
            top_level_query_handler_context=context_mock,
            parameter=sample,
            query_handler_factory=factory,
        )
        output = runner.run()
    assert output.result == sample
