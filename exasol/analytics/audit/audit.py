from typing import List

from exasol.analytics.query_handler.query.select import (
    DB_OBJECT_NAME_TAG,
    DB_OBJECT_TYPE_TAG,
    DB_OPERATION_TYPE_TAG,
    AuditQuery,
    ModifyQuery,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    Column,
    TableName,
    decimal_column,
    timestamp_column,
    varchar_column,
)


class TableDescription:
    def __init__(self, table: TableName, columns: List[Column]):
        self.table = table
        self.columns = {c.name.name: c for c in columns}

    @property
    def create(self):
        columns = ",\n  ".join(c.for_create for c in self.columns.values())
        return cleandoc(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table.fully_qualified} (
              {columns}
            """
        )


class AuditColumn:
    TIMESTAMP = timestamp_column("TIMESTAMP")
    SESSION_ID = decimal_column("SESSION_ID", precision=20)
    JOB_ID = varchar_column("JOB_ID", size=200)
    ROWS_COUNT = decimal_column("ROWS_COUNT", precision=36)
    OBJECT_NAME = varchar_column(DB_OBJECT_NAME_TAG, size=200)
    OBJECT_TYPE = varchar_column(DB_OBJECT_TYPE_TAG, size=200)
    OPERATION_TYPE = varchar_column(DB_OPERATION_TYPE_TAG, size=200)
    OPERATION_ID = decimal_column("OPERATION_ID", precision=36)
    ERROR = varchar_column("ERROR", size=200)
    INFO = varchar_column("INFO", size=200)

    _dict = {
        "TIMESTAMP": TIMESTAMP,
        "SESSION_ID": SESSION_ID,
        "JOB_ID": JOB_ID,
        "ROWS_COUNT": ROWS_COUNT,
        "OBJECT_NAME": OBJECT_NAME,
        "OBJECT_TYPE": OBJECT_TYPE,
        "OPERATION_TYPE": OPERATION_TYPE,
        "OPERATION_ID": OPERATION_ID,
        "ERROR": ERROR,
        "INFO": INFO,
    }

    @classmethod
    def from_name(cls, name: str) -> Column:
        return cls._dict[name]

    @classmethod
    def all(cls) -> List[Column]:
        return list(cls._dict.values())


def status_query(query: ModifyQuery) -> AuditQuery:
    if query.modifies_row_count:
        table_name = query.db_object_name.fully_qualified
        count_query = f"SELECT COUNT(1) AS ROWS_COUNT FROM {table_name}"
        output_columns = [AuditColumn.ROWS_COUNT]
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
