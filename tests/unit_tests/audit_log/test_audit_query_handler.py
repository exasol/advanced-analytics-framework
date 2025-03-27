from dataclasses import dataclass
from typing import Generic
from unittest.mock import Mock

import pytest

from exasol.analytics.audit.audit_query_handler import (
    AuditQueryHandler,
    IllegalMethodCallError,
    ParameterType,
    ResultType,
)
from exasol.analytics.query_handler.query.select import (
    DbOperationType,
    ModifyQuery,
    AuditQuery,
    Query,
    SelectQueryWithColumnDefinition,
)
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
    decimal_column,
)
from tests.utils.audit_table_utils import (
    QueryStringCriterion,
    query_matcher,
    create_insert_query,
    expected_query,
)

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext

@dataclass
class MyParameterType(Generic[ParameterType]):
    db_schema: str
    table_name_prefix: str


AUDIT_TABLE_NAME_PREFIX = "PPP"

AUDIT_TABLE_NAME = TableNameImpl(
    f"{AUDIT_TABLE_NAME_PREFIX}_AUDIT_LOG", SchemaName("SSS")
)

EMPTY_FINISH = Finish(result="finish result", audit_query=None)

def create_audit_query_handler(
    child: QueryHandler[ParameterType, ResultType],
) -> AuditQueryHandler:
    def schema_getter(parameter: MyParameterType) -> str:
        return parameter.db_schema

    def table_name_prefix_getter(parameter: MyParameterType) -> str:
        return parameter.table_name_prefix

    def additional_columns_provider() -> list[Column]:
        return [decimal_column("DDD", precision=9)]

    parameter = MyParameterType(
        db_schema="SSS",
        table_name_prefix=AUDIT_TABLE_NAME_PREFIX,
    )

    return AuditQueryHandler(
        parameter=parameter,
        context=Mock(),
        query_handler_factory=lambda parameter, context: child,
        schema_getter=schema_getter,
        table_name_prefix_getter=table_name_prefix_getter,
        additional_columns_provider=additional_columns_provider,
    )


def test_constructor() -> None:
    child = Mock()
    testee = create_audit_query_handler(child)
    audit_table = testee._audit_table
    assert audit_table.name == AUDIT_TABLE_NAME
    assert '"DDD" DECIMAL(9,0)' in [c.for_create for c in audit_table.columns]
    assert child == testee._child.query_handler


def test_start_finish_no_audit_query():
    """
    Simulate a child query handler immediately returning Finish with no
    audit query.
    Verify result of returned action
    """
    start_method = Mock(return_value=EMPTY_FINISH)
    child = Mock(start=start_method)
    testee = create_audit_query_handler(child)
    action = testee.start()
    assert action == EMPTY_FINISH
    with pytest.raises(IllegalMethodCallError):
        testee.handle_query_result(Mock())


def test_start_finish_with_audit_query():
    """
    Simulate a child query handler immediately returning Finish with an
    audit query. Verify audit query has been rewritten to an insert query.
    """
    event_attributes = '{"a1": 123}'
    audit_query = AuditQuery(audit_fields={"EVENT_ATTRIBUTES": event_attributes })
    start_method = Mock(return_value=Finish(result="finish result", audit_query=audit_query))
    child = Mock(start=start_method)
    testee = create_audit_query_handler(child)
    action_1 = testee.start()
    assert isinstance(action_1, Continue)
    matchers = [
        query_matcher(
            expected_query(AUDIT_TABLE_NAME, DbOperationType.CREATE_IF_NOT_EXISTS),
            QueryStringCriterion.STARTS_WITH,
        ),
        query_matcher(
            expected_query(
                AUDIT_TABLE_NAME,
                query_string_suffix=f" .*'{event_attributes}'",
            ),
            QueryStringCriterion.REGEXP,
        ),
    ]
    for matcher, actual in zip(matchers, action_1.query_list):
        assert actual == matcher
    action_2 = testee.handle_query_result(Mock())
    assert action_2 == EMPTY_FINISH
    with pytest.raises(IllegalMethodCallError):
        testee.handle_query_result(Mock())


def continue_action(query_list: list[Query]) -> Continue:
    input_query = SelectQueryWithColumnDefinition(
        query_string="SELECT 1 as DUMMY_COLUMN",
        output_columns=[decimal_column("DUMMY_COLUMN", precision=10)],
    )
    return Continue(query_list=query_list, input_query=input_query)


def test_start_continue_finish_no_audit_query():
    """
    Simulate a child query handler with start() returning Continue and
    handle_query_result returning Finish.

    Verify ModifyQuery contained in Continue to be augmented with CREATE TABLE
    statement and additional INSERT statemtents into audit log table.
    """

    def create_child(query: Query):
        start_method = Mock(return_value=continue_action([query]))
        hqr_method = Mock(return_value=EMPTY_FINISH)
        return Mock(start=start_method, handle_query_result=hqr_method)

    query = create_insert_query(TableNameImpl("T", SchemaName("S2")), audit=True)
    child = create_child(query)
    expected_queries = [
        expected_query(AUDIT_TABLE_NAME, DbOperationType.CREATE_IF_NOT_EXISTS),
        expected_query(AUDIT_TABLE_NAME),
        expected_query(query.db_object_name, query.db_operation_type),
        expected_query(AUDIT_TABLE_NAME),
    ]

    testee = create_audit_query_handler(child)
    action_1 = testee.start()
    for expected, actual in zip(expected_queries, action_1.query_list):
        actual == query_matcher(expected, QueryStringCriterion.STARTS_WITH)
    action_2 = testee.handle_query_result(Mock())
    assert action_2 == EMPTY_FINISH
