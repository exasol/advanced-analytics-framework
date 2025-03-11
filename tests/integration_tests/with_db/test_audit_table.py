from dataclasses import dataclass

import pytest
from exasol.python_extension_common.deployment.temp_schema import temp_schema

from exasol.analytics.audit.audit import (
    AuditTable,
    TableDescription,
)
from exasol.analytics.audit.columns import BaseAuditColumns
from exasol.analytics.schema import (
    SchemaName,
    TableLikeNameImpl,
    decimal_column,
    varchar_column,
)


@pytest.fixture(scope="session")
def db_schema(pyexasol_connection):
    with temp_schema(pyexasol_connection) as db_schema:
        yield db_schema


@dataclass
class ExaAllColumns:
    connection: pyexasol.ExaConnection
    schema: str

    def query(self, table_name: str) -> dict[str, str]:
        raw = self.connection.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_ALL_COLUMNS"
            f" WHERE COLUMN_SCHEMA='{self.schema}'"
            f" AND COLUMN_TABLE='{table_name}'"
        ).fetchall()
        return {c[0]: c[1] for c in raw}


@pytest.fixture
def exa_all_columns(pyexasol_connection, db_schema):
    return ExaAllColumns(pyexasol_connection, db_schema)


def test_create_table(pyexasol_connection, db_schema, exa_all_columns):
    columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    table = TableDescription(
        table=TableLikeNameImpl("SAMPLE", SchemaName(db_schema)),
        columns=columns,
    )
    pyexasol_connection.execute(table.render_create)
    actual = exa_all_columns.query(table_name="SAMPLE")
    expected = {c.name.name: c.type.rendered for c in columns}
    assert actual == expected


def test_create_audit_table(pyexasol_connection, db_schema, exa_all_columns):
    additional_columns = [
        varchar_column("NAME", size=20),
        decimal_column("AGE", precision=3),
    ]
    audit_table = AuditTable(db_schema, "pfx", additional_columns)
    pyexasol_connection.execute(audit_table.render_create)
    actual = exa_all_columns.query(audit_table.table.name)
    expected = {
        c.name.name: c.type.rendered
        for c in (BaseAuditColumns.all + additional_columns)
    }
    assert actual == expected
