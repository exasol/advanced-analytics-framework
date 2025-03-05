from exasol.analytics.audit.audit import (
    AuditColumn,
    status_query,
)
from exasol.analytics.query_handler.query.select import (
    AuditQuery,
    ModifyQuery,
)
from exasol.analytics.schema import (
    SchemaName,
    TableLikeNameImpl,
)


def test_status_query():
    """
    Create a ModifyQuery with audit=True und indicating that it
    potentially change the number of rows of the modified table
    and verify the results of status_query(query).
    """
    table_name = TableLikeNameImpl(
        schema=SchemaName("SSS"),
        table_like_name="TTT",
    )
    query = ModifyQuery(
        query_string=(
            'INSERT INTO "SSS"."TTT"'
            ' ("RESULT", "ERROR")'
            " VALUES (3, 'E3'), (4, 'E4')"
        ),
        db_object_ref=table_name,
        db_object_type="TABLE",
        db_operation_type="INSERT",
        audit_fields={"INFO": "none"},
        audit=True,
    )
    actual = status_query(query)
    assert isinstance(actual, AuditQuery)
    column = AuditColumn.ROWS_COUNT
    assert actual.select_with_columns.output_columns == [ column ]
    column_name = column.name.fully_qualified
    expected = f"SELECT COUNT(1) AS {column_name} FROM {table_name.fully_qualified}"
    assert actual.query_string == expected
