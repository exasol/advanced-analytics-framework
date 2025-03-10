from exasol.analytics.audit.audit import AuditTable
from exasol.analytics.schema import (
    decimal_column,
    varchar_column,
)


def test_init():
    additional_columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    audit_table = AuditTable("my_schema", "pfx", additional_columns)
    assert audit_table.table.schema_name.name == "my_schema"
    assert audit_table.table.name.startswith("pfx_")
    assert additional_columns == [
        audit_table.columns[c.name.name] for c in additional_columns
    ]
