from dataclasses import dataclass


@dataclass
class QueryHandlerAudit:
    """
    Definition of the audit data provided by a QueryHandler (QH).

    The data can be provided in two ways.
    1. An instance of this class can be included in the `Finish` object returned
       by the `QH.start` or `QH.handle_query_result` in its final iteration.
    2. An AuditQuery can return results with columns matching the attributes of
       this class.
    """
    configuration: str | None = None
    information: str | None = None
    error: str | None = None
