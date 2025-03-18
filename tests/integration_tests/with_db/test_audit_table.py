from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.schema import (
    decimal_column,
    varchar_column,
)


def test_create_audit_table(pyexasol_connection, db_schema, exa_all_columns):
    additional_columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    audit_table = AuditTable(db_schema, "pfx", additional_columns)
    pyexasol_connection.execute(audit_table.create_statement)
    actual = exa_all_columns.query(audit_table.name.name)
    expected = {
        c.name.name: c.type.rendered
        for c in (BaseAuditColumns.all + additional_columns)
    }
    assert actual == expected
