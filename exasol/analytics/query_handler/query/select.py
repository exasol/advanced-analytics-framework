from typing import (
    Any,
    List,
)

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.schema import (
    Column,
    DBObjectName,
    DbObjectType,
    DbOperationType,
)

DB_OBJECT_NAME_TAG = "DB_OBJECT_NAME"
DB_OBJECT_TYPE_TAG = "DB_OBJECT_TYPE"
DB_OPERATION_TYPE_TAG = "DB_OPERATION_TYPE"


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
    ):
        self._select_with_columns = select_with_columns
        AuditData.__init__(self, audit_fields)

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


class ModifyQuery(CustomQuery, AuditData):
    """
    A wrapper for a query that changes data in the database (e.g. INSERT or UPDATE)
    or creates the table (e.g. CREATE TABLE). This type of query is auditable.
    """

    def __init__(
        self,
        query_string: str,
        # Using DBObjectName instead of str, because counting rows of the modified table
        # requires to use fully_qualified
        db_object_name: DBObjectName,
        db_object_type: DbObjectType | str,
        db_operation_type: DbOperationType | str,
        audit_fields: dict[str, Any] | None = None,
        audit: bool = False,
    ):
        CustomQuery.__init__(self, query_string)
        AuditData.__init__(self, audit_fields)
        self.audit_fields[DB_OBJECT_NAME_TAG] = db_object_name
        self.audit_fields[DB_OBJECT_TYPE_TAG] = (
            db_object_type.name
            if isinstance(db_object_type, DbObjectType)
            else db_object_type
        )
        self.audit_fields[DB_OPERATION_TYPE_TAG] = (
            db_operation_type.name
            if isinstance(db_operation_type, DbOperationType)
            else db_operation_type
        )
        self._audit = audit

    @property
    def db_object_name(self) -> DBObjectName:
        return self.audit_fields[DB_OBJECT_NAME_TAG]

    @property
    def db_object_type(self) -> str:
        return self.audit_fields[DB_OBJECT_TYPE_TAG]

    @property
    def db_operation_type(self) -> str:
        return self.audit_fields[DB_OPERATION_TYPE_TAG]

    @property
    def modifies_row_count(self) -> bool:
        """
        This property tells, whether the current ModifyQuery potentially
        modifies the row count of a table. This is only relevant if the
        ModifyQuery modifies a DbObjectType TABLE and is of DbOperationType
        either INSERT or CREATE (with data of a subquery).
        """
        return (self.db_object_type == "TABLE") and (
            self.db_operation_type in ["INSERT", "CREATE"]
        )

    @property
    def audit(self) -> bool:
        return self._audit
