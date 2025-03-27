from dataclasses import dataclass
from typing import Generic
from unittest.mock import Mock

from exasol.analytics.audit.audit_query_handler import (
    AuditQueryHandler,
    ParameterType,
    ResultType,
)
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import Finish
from exasol.analytics.schema import (
    Column,
    decimal_column,
)


@dataclass
class MyParameterType(Generic[ParameterType]):
    db_schema: str
    table_name_prefix: str


def create_audit_query_handler(child: QueryHandler[ParameterType, ResultType]) -> AuditQueryHandler:
    def schema_getter(parameter: MyParameterType) -> str:
        return parameter.db_schema

    def table_name_prefix_getter(parameter: MyParameterType) -> str:
        return parameter.table_name_prefix

    def additional_columns_provider(parameter: MyParameterType) -> list[Column]:
        return [decimal_column("DDD", precision=9)]

    parameter = MyParameterType(db_schema="SSS", table_name_prefix="PPP")
    return AuditQueryHandler(
        parameter=parameter,
        context=Mock(),
        query_handler_factory=Mock(return_value=child),
        schema_getter=schema_getter,
        table_name_prefix_getter=table_name_prefix_getter,
        additional_columns_provider=additional_columns_provider,
    )


def test_constructor() -> None:
    child = Mock()
    testee = create_audit_query_handler(child)
    audit_table = testee._audit_table
    assert audit_table.name.fully_qualified.startswith('"SSS"."PPP')
    assert '"DDD" DECIMAL(9,0)' in [ c.for_create for c in audit_table.columns ]
    assert child == testee._child.query_handler


def test_start_finish():
    start_method = Mock(return_value=Finish(result="some result"))
    child = Mock(start=start_method)
    actual = create_audit_query_handler(child).start()
    assert actual.result == "some result"
