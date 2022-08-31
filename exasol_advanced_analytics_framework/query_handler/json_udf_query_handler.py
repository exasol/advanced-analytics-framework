from typing import Dict, Any

from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler

JSONType = Dict[str, Any]


class JSONQueryHandler(QueryHandler[JSONType, JSONType]):
    pass
