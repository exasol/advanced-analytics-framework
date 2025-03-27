from dataclasses import dataclass
from typing import Generic
from unittest.mock import Mock

from exasol.analytics.audit.audit_query_handler import (
    AuditQueryHandler,
    ParameterType,
)
from exasol.analytics.schema import (
    Column,
    decimal_column,
)


@dataclass
class MyParameterType(Generic[ParameterType]):
    db_schema: str
    table_name_prefix: str


def test_constructor() -> None:
    def schema_getter(parameter: MyParameterType) -> str:
        return parameter.db_schema

    def table_prefix_getter(parameter: MyParameterType) -> str:
        return parameter.table_name_prefix

    def additional_columns_getter(parameter: MyParameterType) -> list[Column]:
        return [decimal_column("DDD", precision=9)]

    parameter = MyParameterType(db_schema="SSS", table_name_prefix="PPP")
    testee = AuditQueryHandler(
        parameter,
        Mock(),
        Mock(),
        schema_getter,
        table_prefix_getter,
        additional_columns_getter,
    )
    audit_table = testee._audit_table
    assert audit_table.name.fully_qualified.startswith('"SSS"."PPP')
    assert '"DDD" DECIMAL(9,0)' in [ c.for_create for c in audit_table.columns ]
