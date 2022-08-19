from dataclasses import dataclass
from typing import List, Optional

from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query_handler \
    import QueryHandler


@dataclass()
class QueryHandlerRunnerState:
    top_level_query_handler_context: TopLevelQueryHandlerContext
    query_handler: QueryHandler
    query_columns: List[Column]
    return_query_query_handler_context: Optional[ScopeQueryHandlerContext] = None
