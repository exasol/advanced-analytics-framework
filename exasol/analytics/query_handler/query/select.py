from typing import List, Any

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import Column


class CustomQuery(Query):
    def __init__(self, query_string: str):
        self._query_string = query_string

    @property
    def query_string(self) -> str:
        return self._query_string


class SelectQuery(CustomQuery):
    """
    A query wrapper for a read-only query. Such query is not auditable.
    """


class SelectQueryWithColumnDefinition(SelectQuery):

    def __init__(self, query_string: str, output_columns: List[Column]):
        super().__init__(query_string)
        self._output_columns = output_columns

    @property
    def output_columns(self) -> List[Column]:
        return self._output_columns


class AuditData:
    """
    This is a collection of data for auditing. The data represent one audit message.
    The items in the dictionary correspond to the columns in the audit table.
    Components at different levels in the call stack can add their own items here.
    """
    def __init__(self, audit_fields: dict[str, Any] | None = None):
        self._audit_fields = audit_fields or {}

    @property
    def audit_fields(self) -> dict[str, Any]:
        return self._audit_fields


class AuditQuery(AuditData):
    """
    A query wrapper for a special read-only query that selects data for auditing.
    """
    def __init__(self, query_with_columns: SelectQueryWithColumnDefinition,
                 audit_fields: dict[str, Any] | None = None):
        super().__init__(audit_fields)
        self._query_with_columns = query_with_columns

    def query_with_columns(self) -> SelectQueryWithColumnDefinition:
        return self._query_with_columns


class WriteQuery(CustomQuery, AuditData):
    """
    A query wrapper for a query that changes data in a database table (e.g. INSERT or
    UPDATE) or creates the table (e.g. CREATE TABLE). This type of query is auditable.
    """
    def __init__(self, query_string: str, audit: bool = False,
                 audit_fields: dict[str, Any] | None = None):
        CustomQuery.__init__(self, query_string)
        AuditData.__init__(self, audit_fields)
        self._audit = audit

    @property
    def audit(self) -> bool:
        return self._audit
