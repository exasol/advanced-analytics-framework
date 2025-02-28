from typing import List, Any

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import Column, TableLikeName

TABLE_NAME_TAG = 'table_name'


class CustomQuery(Query):
    def __init__(self, query_string: str):
        self._query_string = query_string

    @property
    def query_string(self) -> str:
        return self._query_string


class SelectQuery(CustomQuery):
    """
    A wrapper for a read-only query. Such query is not auditable.
    """


class SelectQueryWithColumnDefinition(SelectQuery):
    """
    A wrapper for a read-only query, which also provide a definition of the
    output columns. The query is not auditable.
    """

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


class AuditQuery(SelectQueryWithColumnDefinition, AuditData):
    """
    A wrapper for a special read-only query that selects data for auditing. An object
    of the class can also be used as an audit property bag, since it inherits from the
    `AuditData` as well as `SelectQueryWithColumnDefinition`. The query is optional.
    If provided the output columns should also be defined.
    """
    def __init__(self, query_string: str = '',
                 output_columns: list[Column] | None = None,
                 audit_fields: dict[str, Any] | None = None):
        SelectQueryWithColumnDefinition.__init__(self, query_string, output_columns or [])
        AuditData.__init__(self, audit_fields)

    def __post_init__(self):
        if self.query_string and not self.output_columns:
            raise RuntimeError('No columns defined for an audit query')


class WriteQuery(CustomQuery, AuditData):
    """
    A wrapper for a query that changes data in a database table (e.g. INSERT or UPDATE)
    or creates the table (e.g. CREATE TABLE). This type of query is auditable.
    """
    def __init__(self, query_string: str, affected_table: TableLikeName,
                 audit_fields: dict[str, Any] | None = None, audit: bool = False):
        CustomQuery.__init__(self, query_string)
        AuditData.__init__(self, audit_fields)
        self.audit_fields[TABLE_NAME_TAG] = affected_table
        self._audit = audit

    @property
    def table_name(self) -> TableLikeName:
        return self.audit_fields[TABLE_NAME_TAG]

    @property
    def audit(self) -> bool:
        return self._audit
