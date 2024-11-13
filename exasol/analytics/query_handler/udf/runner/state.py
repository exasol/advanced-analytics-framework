from dataclasses import dataclass
from typing import List, Optional

from exasol.analytics.schema.column import \
    Column

from exasol.analytics.query_handler.context.scope import     ScopeQueryHandlerContext
from exasol.analytics.query_handler.context.top_level_query_handler_context import     TopLevelQueryHandlerContext
from exasol.analytics.query_handler.query_handler     import QueryHandler
from exasol.analytics.query_handler.udf.connection_lookup import UDFConnectionLookup


@dataclass()
class QueryHandlerRunnerState:
    top_level_query_handler_context: TopLevelQueryHandlerContext
    query_handler: QueryHandler
    connection_lookup: UDFConnectionLookup
    input_query_query_handler_context: Optional[ScopeQueryHandlerContext] = None
    input_query_output_columns: Optional[List[Column]] = None
