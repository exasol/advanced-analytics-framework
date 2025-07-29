from typing import (
    Any,
    Dict,
)

from exasol.analytics.query_handler.query_handler import QueryHandler

JSONType = dict[str, Any]


class JSONQueryHandler(QueryHandler[JSONType, JSONType]):
    pass
