from exasol.analytics.schema import (
    decimal_column,
    timestamp_column,
    varchar_column,
)


class BaseAuditColumns:
    TIMESTAMP = timestamp_column("LOG_TIMESTAMP", precision=3)
    SESSION_ID = decimal_column("SESSION_ID", precision=20)
    RUN_ID = decimal_column(
        "RUN_ID",
        precision=20,
        comment="use POSIX_TIME(SYSTIMESTAMP()) * 1000",
    )
    ROWS_COUNT = decimal_column("ROWS_COUNT", precision=36)
    QUERY_HANDLER_ID = decimal_column("QUERY_HANDLER_ID", precision=32)
    QUERY_HANDLER_NAME = varchar_column("QUERY_HANDLER_NAME", size=2000000)
    # QUERY_HANDLER_PHASE: TBC
    SPAN_TYPE = varchar_column("SPAN_TYPE", size=128)
    SPAN_ID = decimal_column("SPAN_ID", precision=32)
    SPAN_DESCRIPTION = varchar_column("SPAN_DESCRIPTION", size=2000000)
    OBJECT_SCHEMA = varchar_column(
        "DB_OBJECT_SCHEMA",
        size=128,
        comment="Optional, can be NULL",
    )
    OBJECT_NAME = varchar_column(
        "DB_OBJECT_NAME",
        size=128,
        comment="Contains the schema name for operations CREATE/DROP SCHEMA.",
    )
    OBJECT_TYPE = varchar_column("DB_OBJECT_TYPE", size=128)
    OPERATION_NAME = varchar_column("DB_OPERATION_TYPE", size=128)
    OPERATION_ID = decimal_column("OPERATION_ID", precision=36)
    ERROR_MESSAGE = varchar_column("ERROR_MESSAGE", size=200)

    all = [
        TIMESTAMP,
        SESSION_ID,
        RUN_ID,
        ROWS_COUNT,
        QUERY_HANDLER_ID,
        QUERY_HANDLER_NAME,
        SPAN_TYPE,
        SPAN_ID,
        SPAN_DESCRIPTION,
        OBJECT_SCHEMA,
        OBJECT_NAME,
        OBJECT_TYPE,
        OPERATION_NAME,
        ERROR_MESSAGE,
    ]

    basic = {
        TIMESTAMP.name.name: "SYSTIMESTAMP()",
        SESSION_ID.name.name: "CURRENT_SESSION",
        RUN_ID.name.name: "POSIX_TIME(SYSTIMESTAMP(9)) * 1000",
    }
