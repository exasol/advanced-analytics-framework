from typing import List

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import Column


class SelectQuery(Query):
    """
    A query wrapper for a read-only query. Such query is not auditable.
    """

    def __init__(self, query_string: str):
        self._query_string = query_string

    @property
    def query_string(self) -> str:
        return self._query_string


class WriteQuery(SelectQuery):
    """
    A query wrapper for a query that changes data in a database table (e.g. INSERT or
    UPDATE) or creates the table (e.g. CREATE TABLE). This type of query is auditable.
    """
    def __init__(self, query_string: str, affected_table: str, audit: bool = False):
        super().__init__(query_string)
        self._affected_table = affected_table
        self._audit = audit

    @property
    def affected_table(self) -> str:
        return self._affected_table

    @property
    def audit(self) -> bool:
        return self._audit


class AuditQuery(SelectQuery):
    """
    A query wrapper for a special read-only query that selects data for auditing.
    The output columns of such a query should correspond to the definition of the
    audit data provided by a QueryHandler.
    """
    pass


class SelectQueryWithColumnDefinition(SelectQuery):

    def __init__(self, query_string: str, output_columns: List[Column]):
        super().__init__(query_string)
        self._output_columns = output_columns

    @property
    def output_columns(self) -> List[Column]:
        return self._output_columns
