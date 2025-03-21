from typing import (
    Iterator,
    cast,
)

from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    ModifyQuery,
    Query,
    SelectQueryWithColumnDefinition,
)
from exasol.analytics.schema import (
    Column,
    ColumnName,
    DBObjectName,
    DBObjectNameWithSchema,
    DbOperationType,
    InsertStatement,
    SchemaName,
    Table,
    TableNameImpl,
)


class AuditTable(Table):
    def __init__(
        self,
        db_schema: str,
        table_name_prefix: str,
        additional_columns: list[Column] = [],
    ):
        if not table_name_prefix:
            raise ValueError("table_name_prefix must not be empty")
        table_name = f"{table_name_prefix}_AUDIT_LOG"
        super().__init__(
            name=TableNameImpl(table_name, SchemaName(db_schema)),
            columns=(BaseAuditColumns.all + additional_columns),
        )
        self._column_names = [c.name for c in self.columns]

    def augment(self, queries: Iterator[Query]) -> Iterator[str]:
        """
        Process the specified queries and intermerge insert statements
        into the Audit Log if requested:

        * Queries not requesting any entry into the Audit Log are simply executed.

        * Instances of :class:`AuditQuery` are inserted into the Audit Log,
          optionally including custom audit_fields and a subquery
          (SelectQueryWithColumnDefinition)

        * Instances of :class:`ModifyQuery` requesting an entry into the Audit
          Log are wrapped into one insert statement before and one after. The
          actual modifying query in between remains unchanged.  The insert
          statements before and after record the timestamp and, if the
          ModifyQuery represents an INSERT operation, the number of rows in
          the modified table.
        """
        for query in queries:
            if not query.audit:
                yield query.query_string
            elif isinstance(query, AuditQuery):
                yield self._insert(query)
            elif isinstance(query, ModifyQuery):
                yield from self._wrap(query)
            else:
                raise TypeError(
                    f"Unexpected type {type(query).__name__}"
                    f' of query "{query.query_string}"'
                )

    def _insert(self, query: AuditQuery) -> str:
        insert_statement = (
            InsertStatement(self._column_names, separator=",\n  ")
            .add_constants(query.audit_fields)
            .add_scalar_functions(BaseAuditColumns.values)
        )

        suffix = ""
        if query.select_with_columns:
            alias = TableNameImpl("SUB_QUERY")
            subquery_columns = {
                c.name.name: ColumnName(c.name.name, alias)
                for c in query.select_with_columns.output_columns
            }
            insert_statement.add_references(subquery_columns)
            suffix = (
                "\nFROM VALUES (1) CROSS JOIN\n"
                f"  ({query.query_string}) as {alias.fully_qualified}"
            )

        return (
            f"INSERT INTO {self.name.fully_qualified} (\n"
            f"  {insert_statement.columns}\n"
            ") SELECT\n"
            f"  {insert_statement.values}{suffix}"
        )

    def _wrap(self, query: ModifyQuery) -> Iterator[str]:
        """
        Wrap the specified ModifyQuery it into 2 queries recording the
        state before and after the actual ModifyQuery.

        The state includes timestamps and optionally the number of rows of the
        modified table, in case the ModifyQuery indicates potential changes to the
        number of rows.
        """
        if query.db_operation_type != DbOperationType.INSERT:
            yield query.query_string
        else:
            yield from [
                self._count_rows(query, "Begin"),
                query.query_string,
                self._count_rows(query, "End"),
            ]

    def _count_rows(self, query: ModifyQuery, event_name: str) -> str:
        """
        Create an SQL INSERT statement counting the rows of the table
        modified by ModifyQuery `query` and populate columns in the Audit
        Table marked with "+":

        + LOG_TIMESTAMP: BaseAuditColumns.values
        + SESSION_ID: BaseAuditColumns.values
        - RUN_ID
        + ROW_COUNT: subquery
        + LOG_SPAN_NAME: query.db_operation_type: DbOperationType
        - LOG_SPAN_ID
        - PARENT_LOG_SPAN_ID
        + EVENT_NAME: parameter event_name
        - EVENT_ATTRIBUTES
        + OBJECT_TYPE: query.db_object_type: DbObjectType
        + OBJECT_SCHEMA: query.db_object_name: DBObjectName
        + OBJECT_NAME: query.db_object_name: DBObjectName
        - ERROR_MESSAGE
        """

        def schema(db_obj: DBObjectName) -> str | None:
            if not isinstance(db_obj, DBObjectNameWithSchema):
                return None
            schema = cast(DBObjectNameWithSchema, db_obj).schema_name
            return schema.name if schema else None

        db_obj = query.db_object_name
        query_attributes = {
            "LOG_SPAN_NAME": query.db_operation_type.name,
            "EVENT_NAME": event_name,
            "DB_OBJECT_TYPE": query.db_object_type.name,
            "DB_OBJECT_SCHEMA": schema(db_obj),
            "DB_OBJECT_NAME": db_obj.name,
        }
        other_table = query.db_object_name.fully_qualified
        row_count = BaseAuditColumns.ROW_COUNT.name.name
        insert_statement = (
            InsertStatement(self._column_names, separator=",\n  ")
            .add_scalar_functions(BaseAuditColumns.values)
            .add_constants(query_attributes)
            .add_constants(query.audit_fields)
            .add_scalar_functions({row_count: f"(SELECT count(1) FROM {other_table})"})
        )
        return (
            f"INSERT INTO {self.name.fully_qualified} (\n"
            f"  {insert_statement.columns}\n"
            ") SELECT\n"
            f"  {insert_statement.values}"
        )
