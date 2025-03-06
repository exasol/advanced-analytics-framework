from inspect import cleandoc
from typing import List

from exasol.analytics.audit.columns import AuditColumns
from exasol.analytics.query_handler.query.select import (
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
