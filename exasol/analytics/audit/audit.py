from typing import List

from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.schema import (
    Column,
    SchemaName,
    Table,
    TableNameImpl,
)


class AuditTable(Table):
    def __init__(
        self,
        db_schema: str,
        table_name_prefix: str,
        additional_columns: List[Column] = [],
    ):
        if not table_name_prefix:
            raise ValueError("table_name_prefix must not be empty")
        table_name = f"{table_name_prefix}_AUDIT_LOG"
        super().__init__(
            name=TableNameImpl(table_name, SchemaName(db_schema)),
            columns=(BaseAuditColumns.all + additional_columns),
        )
