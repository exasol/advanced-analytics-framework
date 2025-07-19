from exasol.analytics.audit.audit import BaseAuditColumns
from exasol.analytics.schema import Column


def test_access_by_name():
    assert getattr(BaseAuditColumns, "ERROR_MESSAGE") == BaseAuditColumns.ERROR_MESSAGE


def test_all():
    names = [c.name.name for c in BaseAuditColumns.all]
    assert "LOG_TIMESTAMP" in names
    assert "ERROR_MESSAGE" in names
