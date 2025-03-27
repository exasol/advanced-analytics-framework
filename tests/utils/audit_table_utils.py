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
    query_string_suffix: str = "",
) -> Query:
    fqn = table_name.fully_qualified
    query_strings = {
        "INSERT": f"INSERT INTO {fqn}",
        "CREATE_IF_NOT_EXISTS": f"CREATE TABLE IF NOT EXISTS {fqn}",
    }
    return ModifyQuery(
        query_string=query_strings[db_operation_type.name] + query_string_suffix,
        db_object_type=DbObjectType.TABLE,
        db_object_name=table_name,
        db_operation_type=db_operation_type,
    )


class query_matcher:
    """
    Given an expected query template and a match criterion this matcher
    serves in test cases to compare actual queries with the template.
    """
    def __init__(
        self,
        query: Query,
        criterion: QueryStringCriterion = QueryStringCriterion.REGEXP,
    ):
        self.query = query
        self.criterion = criterion

    def __ne__(self, other: Any):
        return not self.__eq__(other)

    def __eq__(self, other: Any):
        if not isinstance(other, self.query.__class__):
            return False

        if not (
            other.query_string.startswith(self.query.query_string)
            if self.criterion == QueryStringCriterion.STARTS_WITH
            else re.match(self.query.query_string, other.query_string, re.DOTALL)
        ):
            return False

        if isinstance(other, ModifyQuery):
            return (
                other.db_object_type == self.query.db_object_type
                and other.db_object_name == self.query.db_object_name
                and other.db_operation_type == self.query.db_operation_type
            )
        return True
