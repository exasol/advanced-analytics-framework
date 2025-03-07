from exasol.analytics.audit.audit import AuditColumns
from exasol.analytics.schema import Column


def test_docstring():
    attributes = dir(AuditColumns)
    assert "ROWS_COUNT" in attributes
    assert "OBJECT_NAME" in attributes


def test_access_by_name():
    assert getattr(AuditColumns, "ERROR_MESSAGE") == AuditColumns.ERROR_MESSAGE


def test_all():
    names = [c.name.name for c in AuditColumns.all]
    assert "LOG_TIMESTAMP" in names
    assert "ERROR_MESSAGE" in names
