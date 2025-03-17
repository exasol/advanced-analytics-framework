from inspect import cleandoc
from typing import List

from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.schema import (
    Column,
    SchemaName,
    TableLikeName,
    TableLikeNameImpl,
)


class TableDescription:
    """
    This class describes an SQL table by its attributes
    * table (instance of (TableLikeName)) defining the name and optionally the
      database schema of the SQL table
    * columns the names and types of the columns of the SQL table

    The class also offers a property `render_create` which renders the
    attributes into an SQL CREATE TABLE statement.
    """

    def __init__(self, table: TableLikeName, columns: List[Column]):
        self.table = table
        self.columns = {c.name.name: c for c in columns}

    @property
    def render_create(self):
        columns = ",\n  ".join(c.for_create for c in self.columns.values())
        return cleandoc(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table.fully_qualified} (
              {columns}
            )
            """
        )


class AuditTable(TableDescription):
    def __init__(
        self,
        db_schema: str,
        table_name_prefix: str = "",
        additional_columns: List[Column] = [],
    ):
        table_name = "_".join(a for a in [table_name_prefix, "AUDIT_LOG"] if a)
        super().__init__(
            table=TableLikeNameImpl(table_name, SchemaName(db_schema)),
            columns=(BaseAuditColumns.all + additional_columns),
        )
