from __future__ import annotations

import uuid
from typing import Any

from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import (
    Column,
    DBObjectName,
    DbObjectType,
    DbOperationType,
)


def _generate_log_span_id():
    return uuid.uuid4()


class LogSpan:
    """
    A LogSpan represents a span of time in the Audit Log. Each LogSpan has
    a name and an ID.  LogSpans can be nested and each child LogSpan can refer
    to its parent by specifying the ID of the parent LogSpan.

    LOG_SPAN IDs are UUIDs with 128 bit = 32 hex digits > 38 decimal digits.
    """

    def __init__(
        self,
        name: str,
        id: uuid.UUID | None = None,
        parent: LogSpan | None = None,
    ):
        self.name = name
        self.id = id or _generate_log_span_id()
        self.parent = parent

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LogSpan):
            return False
        return (
            self.name == other.name
            and self.id == other.id
            and self.parent == other.parent
        )

    def child(self, name: str, id: uuid.UUID | None = None) -> LogSpan:
        return LogSpan(name, id, parent=self)


class CustomQuery(Query):
    def __init__(self, query_string: str):
        self._query_string = query_string

    @property
    def query_string(self) -> str:
        return self._query_string


class SelectQuery(CustomQuery):
    """
    Read-only query, not auditable.
    """


class SelectQueryWithColumnDefinition(SelectQuery):
    """
    Read-only query incl. output columns. The query is not auditable.
    """

    def __init__(self, query_string: str, output_columns: list[Column]):
        super().__init__(query_string)
        self._output_columns = output_columns

    @property
    def output_columns(self) -> list[Column]:
        return self._output_columns


class AuditData:
    """
    This is a collection of data for auditing. The data represent one audit message.
    The items in the dictionary correspond to the columns in the audit table.
    Components at different levels in the call stack can add their own items here.
    """

    def __init__(
        self,
        audit_fields: dict[str, Any] | None = None,
    ):
        self._audit_fields = audit_fields or {}

    @property
    def audit_fields(self) -> dict[str, Any]:
        return self._audit_fields


class AuditQuery(Query, AuditData):
    """
    A wrapper for a special read-only query that selects data for auditing. An object
    of the class can also be used as an audit property bag, since it inherits from the
    `AuditData`. The provided query with columns are optional.
    """

    def __init__(
        self,
        select_with_columns: SelectQueryWithColumnDefinition | None = None,
        audit_fields: dict[str, Any] | None = None,
        log_span: LogSpan | None = None,
    ):
        AuditData.__init__(self, audit_fields)
        self._select_with_columns = select_with_columns
        self._log_span = log_span

    @property
    def select_with_columns(self) -> SelectQueryWithColumnDefinition | None:
        return self._select_with_columns

    @property
    def query_string(self) -> str:
        return (
            self.select_with_columns.query_string
            if self.select_with_columns is not None
            else "SELECT 1"
        )

    @property
    def audit(self) -> bool:
        return True

    @property
    def log_span(self) -> LogSpan | None:
        return self._log_span


class ModifyQuery(CustomQuery, AuditData):
    """
    A wrapper for a query that changes data in the database (e.g. INSERT or UPDATE)
    or creates the table (e.g. CREATE TABLE). This type of query is auditable.
    """

    def __init__(
        self,
        query_string: str,
        db_object_type: DbObjectType,
        db_object_name: DBObjectName,
        db_operation_type: DbOperationType,
        audit_fields: dict[str, Any] | None = None,
        audit: bool = False,
        parent_log_span: LogSpan | None = None,
    ):
        CustomQuery.__init__(self, query_string)
        AuditData.__init__(self, audit_fields)
        self._db_object_type = db_object_type
        self._db_object_name = db_object_name
        self._db_operation_type = db_operation_type
        self._audit = audit
        # parent_log_span.child() can only be used if parent_log_span is not
        # None, but ModifyQuery needs to create an individual instance of
        # LogSpan in any case.
        self._log_span = LogSpan(db_operation_type.name, parent=parent_log_span)

    @property
    def db_object_type(self) -> DbObjectType:
        return self._db_object_type

    @property
    def db_object_name(self) -> DBObjectName:
        return self._db_object_name

    @property
    def db_operation_type(self) -> DbOperationType:
        return self._db_operation_type

    @property
    def log_span(self) -> LogSpan:
        return self._log_span

    @property
    def modifies_row_count(self) -> bool:
        """
        This property tells, whether the current ModifyQuery potentially
        modifies the row count of a table. This is only relevant if the
        ModifyQuery modifies a DbObjectType TABLE and uses a DbOperationType
        from the list named below, e.g. INSERT.
        """
        return (self.db_object_type == DbObjectType.TABLE) and (
            self.db_operation_type
            in [
                DbOperationType.CREATE,
                DbOperationType.CREATE_OR_REPLACE,
                DbOperationType.CREATE_IF_NOT_EXISTS,
                DbOperationType.DROP,
                DbOperationType.INSERT,
            ]
        )

    @property
    def audit(self) -> bool:
        return self._audit
