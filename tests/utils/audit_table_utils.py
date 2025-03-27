import re
import uuid
from enum import (
    Enum,
    auto,
)
from typing import (
    Any,
    Iterator,
)

import pyexasol

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.select import (
    LogSpan,
    ModifyQuery,
)
from exasol.analytics.schema import (
    DbObjectType,
    DbOperationType,
    TableName,
)


def all_rows_as_dicts(
    pyexasol_connection: pyexasol.ExaConnection,
    table_name: TableName,
) -> Iterator[dict[str, Any]]:
    """
    Uses EXA_ALL_COLUMNS to retrieve the columns of the specified table
    and returns all rows of the table. Each row is represented as a dict using
    the column names as keys.
    """
    column_names = [
        r[0]
        for r in pyexasol_connection.execute(
            "SELECT COLUMN_NAME FROM EXA_ALL_COLUMNS"
            f" WHERE COLUMN_SCHEMA='{table_name.schema_name.name}'"
            f" AND COLUMN_TABLE='{table_name.name}'"
        )
    ]
    comma_sep = ", ".join(f'"{col}"' for col in column_names)
    yield from (
        dict(zip(column_names, values))
        for values in pyexasol_connection.execute(
            f"select {comma_sep} from {table_name.fully_qualified}"
        )
    )


SAMPLE_UUID = uuid.uuid4()
SAMPLE_LOG_SPAN = LogSpan("sample log span")


def create_insert_query(
    table: TableName,
    audit: bool,
    query_string: str = "",
    parent_log_span: LogSpan | None = None,
) -> ModifyQuery:
    return ModifyQuery(
        query_string=query_string
        or (
            f"INSERT INTO {table.fully_qualified}"
            ' ("RESULT", "ERROR")'
            " VALUES (3, 'E3'), (4, 'E4')"
        ),
        db_object_name=table,
        db_object_type=DbObjectType.TABLE,
        db_operation_type=DbOperationType.INSERT,
        audit_fields={"EVENT_ATTRIBUTES": '{"a": 123, "b": "value"}'},
        audit=audit,
        parent_log_span=parent_log_span,
    )


class QueryStringCriterion(Enum):
    REGEXP = auto()
    STARTS_WITH = auto()


def expected_query(
    table_name: TableName,
    db_operation_type: DbOperationType = DbOperationType.INSERT,
) -> Query:
    query_strings = {
        DbOperationType.INSERT: f"INSERT INTO {table_name.fully_qualified}",
        DbOperationType.CREATE_IF_NOT_EXISTS: f"CREATE TABLE IF NOT EXISTS {table_name.fully_qualified}",
    }
    return ModifyQuery(
        query_string=query_strings[db_operation_type],
        db_object_type=DbObjectType.TABLE,
        db_object_name=table_name,
        db_operation_type=db_operation_type,
    )


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
