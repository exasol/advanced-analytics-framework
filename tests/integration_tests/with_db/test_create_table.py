from exasol.analytics.schema import (
    Column,
    DecimalColumn,
    SchemaName,
    Table,
    TableNameImpl,
    VarCharColumn,
)


def test_create_table(pyexasol_connection, db_schema, exa_all_columns):
    columns = [
        VarCharColumn.simple("NAME", size=20),
        DecimalColumn.simple("AGE", precision=3),
    ]
    table = Table(
        name=TableNameImpl("SAMPLE", SchemaName(db_schema)),
        columns=columns,
    )
    pyexasol_connection.execute(table.create_statement)
    actual = exa_all_columns.query(table_name="SAMPLE")
    expected = {c.name.name: c.type.rendered for c in columns}
    assert actual == expected
