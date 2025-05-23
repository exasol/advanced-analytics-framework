from exasol.analytics.schema import (
    DecimalColumn,
    HashSizeUnit,
    HashTypeColumn,
    TimeStampColumn,
    VarCharColumn,
)


class BaseAuditColumns:
    LOG_TIMESTAMP = TimeStampColumn.simple("LOG_TIMESTAMP", precision=3)
    SESSION_ID = DecimalColumn.simple("SESSION_ID", precision=20)
    # RUN_ID must be obtained initially and remain unchanged during lifetime
    # of AuditLogger. AuditLogger sets it from uuid.uuid4().
    RUN_ID = HashTypeColumn.simple("RUN_ID", size=16, unit=HashSizeUnit.BYTE)
    ROW_COUNT = DecimalColumn.simple("ROW_COUNT", precision=36)
    # LOG_SPAN_NAME and LOG_SPAN_ID need to be generated and provided by the
    # creator of the AuditQuery, i.e. lower level query_handlers.

    # For ModifyQuery LOG_SPAN_NAME will be set to the Operation Type, e.g.
    # CREATE_TABLE, CREATE_TABLE, INSERT. For other queries it can be a custom
    # string indicating a specific execution phase.
    LOG_SPAN_NAME = VarCharColumn.simple("LOG_SPAN_NAME", size=2000000)
    # SPAN IDs are UUIDs with 128 bit = 32 hex digits > 38 decimal digits
    LOG_SPAN_ID = HashTypeColumn.simple("LOG_SPAN_ID", size=16, unit=HashSizeUnit.BYTE)
    PARENT_LOG_SPAN_ID = HashTypeColumn.simple(
        "PARENT_LOG_SPAN_ID", size=16, unit=HashSizeUnit.BYTE
    )
    # For ModifyQuery EVENT_NAME will be either "Begin" or "End".  For other
    # queries this can be a custom string, e.g.  "ERROR", "COMMIT", ...
    EVENT_NAME = VarCharColumn.simple("EVENT_NAME", size=128)
    # This will contain the string representation of a json document.
    EVENT_ATTRIBUTES = VarCharColumn.simple("EVENT_ATTRIBUTES", size=2000000)
    DB_OBJECT_TYPE = VarCharColumn.simple("DB_OBJECT_TYPE", size=128)
    # Optional, can be NULL:
    DB_OBJECT_SCHEMA = VarCharColumn.simple("DB_OBJECT_SCHEMA", size=128)
    # Contains the schema name for operations CREATE/DROP SCHEMA:
    DB_OBJECT_NAME = VarCharColumn.simple("DB_OBJECT_NAME", size=128)
    ERROR_MESSAGE = VarCharColumn.simple("ERROR_MESSAGE", size=200)

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
