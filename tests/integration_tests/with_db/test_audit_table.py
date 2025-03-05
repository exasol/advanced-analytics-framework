from exasol.python_extension_common.deployment.temp_schema import temp_schema

from exasol.analytics.audit.audit import (
    AuditColumns,
    AuditTable,
)


@pytest.fixture(scope="session")
def db_schema(pyexasol_connection):
    with temp_schema(pyexasol_connection) as db_schema:
        yield db_schema


def test_create_audit_table(pyexasol_connection, db_schema):
    audit_table = AuditTable(db_schema)
    pyexasol_connection.execute(audit_table.create)
    columns = pyexasol_connection.execute(
        f"DESCRIBE {audit_table.table.fully_qualified}"
    ).fetchall()
    names = [c[0] for c in columns]
    for c in AuditColumns.all:
        assert c.name.name in names
