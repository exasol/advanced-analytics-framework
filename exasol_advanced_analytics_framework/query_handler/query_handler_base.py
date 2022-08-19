from abc import ABC, abstractmethod
from typing import Any, Dict

from exasol_advanced_analytics_framework.query_result.query_result_base import QueryResult
from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query_handler_result \
    import QueryHandlerResultBase


class QueryHandlerBase(ABC):
    def __init__(self, parameters: Dict[str, Any]):
        self.parameters = parameters

    @abstractmethod
    def handle_event(
            self,
            query_result: QueryResult,
            query_handler_context: ScopeQueryHandlerContext) \
            -> QueryHandlerResultBase:
        raise NotImplementedError

    def cleanup(self):
        pass
