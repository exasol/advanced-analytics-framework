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
    ROW_COUNT = decimal_column("ROW_COUNT", precision=36)
    # For ModifyQuery EVENT_NAME will be set to the Operation Type, optionally
    # prefixed by "Before " or "After ". Sample values
    # "Before CREATE_TABLE", "After CREATE_TABLE",
    # "Before INSERT", "After INSERT",
    # ERROR, COMMIT, CUSTOM:
    EVENT_NAME = varchar_column("EVENT_NAME", size=128)
    # SPAN_NAME and SPAN_ID need to be generated and provided by the creator
    # of the AuditQuery, i.e. lower level query_handlers.
    SPAN_NAME = varchar_column("LOG_SPAN_NAME", size=2000000)
    SPAN_ID = decimal_column("LOG_SPAN_ID", precision=32)
    PARENT_SPAN_ID = decimal_column("PARENT_LOG_SPAN_ID", precision=32)
    EVENT_ATTRIBUTES = varchar_column("EVENT_ATTRIBUTES", size=2000000)
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
        ROW_COUNT,
        EVENT_NAME,
        SPAN_NAME,
        SPAN_ID,
        PARENT_SPAN_ID,
        EVENT_ATTRIBUTES,
        OBJECT_SCHEMA,
        OBJECT_NAME,
        OBJECT_TYPE,
        ERROR_MESSAGE,
    ]

    values = {
        TIMESTAMP.name.name: "SYSTIMESTAMP()",
        SESSION_ID.name.name: "CURRENT_SESSION",
    }
