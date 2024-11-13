from typing import Dict, Any

from exasol.analytics.query_handler.query_handler import QueryHandler

JSONType = Dict[str, Any]


class JSONQueryHandler(QueryHandler[JSONType, JSONType]):
    pass
