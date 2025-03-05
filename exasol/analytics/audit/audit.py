from inspect import cleandoc
from typing import List

from exasol.analytics.query_handler.query.select import (
    DB_OBJECT_NAME_TAG,
    DB_OBJECT_SCHEMA_TAG,
    DB_OBJECT_TYPE_TAG,
    DB_OPERATION_TYPE_TAG,
    AuditQuery,
    ModifyQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    Column,
    SchemaName,
    TableLikeName,
    TableLikeNameImpl,
    decimal_column,
    timestamp_column,
    varchar_column,
)


class TableDescription:
    def __init__(self, table: TableLikeName, columns: List[Column]):
        self.table = table
        self.columns = {c.name.name: c for c in columns}

    @property
    def create(self):
        columns = ",\n  ".join(c.for_create for c in self.columns.values())
        return cleandoc(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table.fully_qualified} (
              {columns}
            )
            """
        )


class AuditColumns:
    TIMESTAMP = timestamp_column("LOG_TIMESTAMP", precision=3)
    SESSION_ID = decimal_column("SESSION_ID", precision=20)
    RUN_ID = decimal_column("RUN_ID", precision=20)
    ROWS_COUNT = decimal_column(
        "ROWS_COUNT",
        precision=36,
        comment="use POSIX_TIME(SYSTIMESTAMP()) * 1000",
    )
    QUERY_HANDLER_ID = decimal_column("QUERY_HANDLER_ID", precision=32)
    QUERY_HANDLER_NAME = varchar_column("QUERY_HANDLER_NAME", size=2000000)
    # QUERY_HANDLER_PHASE: TBC
    SPAN_TYPE = varchar_column("SPAN_TYPE", size=128)
    SPAN_ID = decimal_column("SPAN_ID", precision=32)
    SPAN_DESCRIPTION = varchar_column("SPAN_DESCRIPTION", size=2000000)
    OBJECT_SCHEMA = varchar_column(
        DB_OBJECT_SCHEMA_TAG,
        size=128,
        comment="Contains the schema name for operations CREATE/DROP SCHEMA",
    )
    OBJECT_NAME = varchar_column(DB_OBJECT_NAME_TAG, size=128)
    OBJECT_TYPE = varchar_column(DB_OBJECT_TYPE_TAG, size=128)
    OPERATION_NAME = varchar_column(DB_OPERATION_TYPE_TAG, size=128)
    OPERATION_ID = decimal_column("OPERATION_ID", precision=36)
    ERROR_MESSAGE = varchar_column("ERROR_MESSAGE", size=200)

    all = [
        TIMESTAMP,
        SESSION_ID,
        RUN_ID,
        ROWS_COUNT,
        QUERY_HANDLER_ID,
        QUERY_HANDLER_NAME,
        SPAN_TYPE,
        SPAN_ID,
        SPAN_DESCRIPTION,
        OBJECT_SCHEMA,
        OBJECT_NAME,
        OBJECT_TYPE,
        OPERATION_NAME,
        OPERATION_ID,
        ERROR_MESSAGE,
    ]


class AuditTable(TableDescription):
    def __init__(self, db_schema: str):
        super().__init__(
            table=TableLikeNameImpl("AUDIT_LOG", SchemaName(db_schema)),
            columns=AuditColumns.all,
        )


def status_query(query: ModifyQuery) -> AuditQuery:
    if query.modifies_row_count:
        column = AuditColumns.ROWS_COUNT
        table_name = query.db_object_ref.fully_qualified
        column_name = column.name.fully_qualified
        count_query = f"SELECT COUNT(1) AS {column_name} FROM {table_name}"
        output_columns = [column]
    else:
        count_query = "SELECT 1"
        output_columns = []
    select_query = SelectQueryWithColumnDefinition(
        query_string=count_query,
        output_columns=output_columns,
    )
    return AuditQuery(
        select_with_columns=select_query,
        audit_fields=query.audit_fields,
    )
