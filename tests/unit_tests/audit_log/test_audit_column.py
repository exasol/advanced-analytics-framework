from exasol.analytics.audit.audit import AuditColumn
from exasol.analytics.schema import Column


def test_docstring():
    attributes = dir(AuditColumn)
    assert "ROWS_COUNT" in attributes
    assert "OBJECT_NAME" in attributes


def test_access_by_name():
    assert getattr(AuditColumn, "ERROR_MESSAGE") == AuditColumn.ERROR_MESSAGE


def test_all():
    names = [c.name.name for c in AuditColumn.all()]
    assert "LOG_TIMESTAMP" in names
    assert "ERROR_MESSAGE" in names
