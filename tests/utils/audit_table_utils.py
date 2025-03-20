from typing import (
    Any,
    Iterator,
)

import pyexasol

from exasol.analytics.query_handler.query.select import ModifyQuery
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


def create_insert_query(table: TableName, audit: bool):
    return ModifyQuery(
        query_string=(
            f"INSERT INTO {table.fully_qualified}"
            ' ("RESULT", "ERROR")'
            " VALUES (3, 'E3'), (4, 'E4')"
        ),
        db_object_name=table,
        db_object_type=DbObjectType.TABLE,
        db_operation_type=DbOperationType.INSERT,
        audit_fields={"EVENT_ATTRIBUTES": '{"a": 123, "b": "value"}'},
        audit=audit,
    )
