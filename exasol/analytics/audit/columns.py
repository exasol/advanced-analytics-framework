from exasol.analytics.schema import (
    decimal_column,
    timestamp_column,
    varchar_column,
)


class BaseAuditColumns:
    TIMESTAMP = timestamp_column("LOG_TIMESTAMP", precision=3)
    SESSION_ID = decimal_column("SESSION_ID", precision=20)
    # RUN_ID must be obtained initially and remain unchanged during lifetime
    # of AuditLogger. AuditLogger must inherit from QueryHandler and its first
    # task is to get the RUN_ID.
    # Proposed value: POSIX_TIME(SYSTIMESTAMP(9)) * 1000
    RUN_ID = decimal_column("RUN_ID", precision=20)
    ROWS_COUNT = decimal_column("ROWS_COUNT", precision=36)
    # For ModifyQuery EVENT_TYPE will be set to the Operation Type, e.g.
    # CREATE_TABLE, INSERT, ERROR, COMMIT, CUSTOM:
    EVENT_TYPE = varchar_column("EVENT_TYPE", size=128)
    # EVENT_ID needs to be generated and provided by the creator of the
    # AuditQuery, i.e. lower level query_handlers.
    EVENT_ID = decimal_column("EVENT_ID", precision=32)
    PARENT_EVENT_ID = decimal_column("PARENT_EVENT_ID", precision=32)
    EVENT_DESCRIPTION = varchar_column("EVENT_DESCRIPTION", size=2000000)
    OBJECT_TYPE = varchar_column("DB_OBJECT_TYPE", size=128)
    # Optional, can be NULL:
    OBJECT_SCHEMA = varchar_column("DB_OBJECT_SCHEMA", size=128)
    # Contains the schema name for operations CREATE/DROP SCHEMA:
    OBJECT_NAME = varchar_column("DB_OBJECT_NAME", size=128)
    ERROR_MESSAGE = varchar_column("ERROR_MESSAGE", size=200)

    all = [
        TIMESTAMP,
        SESSION_ID,
        RUN_ID,
        ROWS_COUNT,
        EVENT_TYPE,
        EVENT_ID,
        PARENT_EVENT_ID,
        EVENT_DESCRIPTION,
        OBJECT_SCHEMA,
        OBJECT_NAME,
        OBJECT_TYPE,
        ERROR_MESSAGE,
    ]

    values = {
        TIMESTAMP.name.name: "SYSTIMESTAMP()",
        SESSION_ID.name.name: "CURRENT_SESSION",
    }
