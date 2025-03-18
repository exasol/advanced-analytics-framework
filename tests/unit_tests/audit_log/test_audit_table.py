import pytest

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
    assert audit_table.name.schema_name.name == "my_schema"
    assert audit_table.name.name.startswith("pfx_")
    assert set(additional_columns) <= set(audit_table.columns)


def test_empty_prefix():
    with pytest.raises(ValueError):
        AuditTable("my_schema", "")
