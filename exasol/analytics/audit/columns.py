from exasol.analytics.schema import (
    decimal_column,
    timestamp_column,
    varchar_column,
)


class BaseAuditColumns:
    LOG_TIMESTAMP = timestamp_column("LOG_TIMESTAMP", precision=3)
    SESSION_ID = decimal_column("SESSION_ID", precision=20)
    # RUN_ID must be obtained initially and remain unchanged during lifetime
    # of AuditLogger. AuditLogger must inherit from QueryHandler and its first
    # task is to get the RUN_ID.
    # Proposed value: POSIX_TIME(SYSTIMESTAMP(9)) * 1000
    RUN_ID = decimal_column("RUN_ID", precision=20)
    ROW_COUNT = decimal_column("ROW_COUNT", precision=36)
    # LOG_SPAN_NAME and LOG_SPAN_ID need to be generated and provided by the
    # creator of the AuditQuery, i.e. lower level query_handlers.

    # For ModifyQuery LOG_SPAN_NAME will be set to the Operation Type, e.g.
    # CREATE_TABLE, CREATE_TABLE, INSERT. For other queries it can be a custom
    # string indicating a specific execution phase.
    LOG_SPAN_NAME = varchar_column("LOG_SPAN_NAME", size=2000000)
    LOG_SPAN_ID = decimal_column("LOG_SPAN_ID", precision=32)
    PARENT_LOG_SPAN_ID = decimal_column("PARENT_LOG_SPAN_ID", precision=32)
    # For ModifyQuery EVENT_NAME will be either "Begin" or "End".  For other
    # queries this can be a custom string, e.g.  "ERROR", "COMMIT", ...
    EVENT_NAME = varchar_column("EVENT_NAME", size=128)
    # This will contain the string representation of a json document.
    EVENT_ATTRIBUTES = varchar_column("EVENT_ATTRIBUTES", size=2000000)
    DB_OBJECT_TYPE = varchar_column("DB_OBJECT_TYPE", size=128)
    # Optional, can be NULL:
    DB_OBJECT_SCHEMA = varchar_column("DB_OBJECT_SCHEMA", size=128)
    # Contains the schema name for operations CREATE/DROP SCHEMA:
    DB_OBJECT_NAME = varchar_column("DB_OBJECT_NAME", size=128)
    ERROR_MESSAGE = varchar_column("ERROR_MESSAGE", size=200)

    all = [
        LOG_TIMESTAMP,
        SESSION_ID,
        RUN_ID,
        ROW_COUNT,
        LOG_SPAN_NAME,
        LOG_SPAN_ID,
        PARENT_LOG_SPAN_ID,
        EVENT_NAME,
        EVENT_ATTRIBUTES,
        DB_OBJECT_SCHEMA,
        DB_OBJECT_NAME,
        DB_OBJECT_TYPE,
        ERROR_MESSAGE,
    ]

    values = {
        LOG_TIMESTAMP.name.name: "SYSTIMESTAMP()",
        SESSION_ID.name.name: "CURRENT_SESSION",
    }
