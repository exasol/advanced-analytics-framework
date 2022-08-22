from abc import ABC, abstractmethod
from typing import Any, Dict, TypeVar, Generic, Union

from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.result \
    import Result, Continue, Finish

ResultType = TypeVar("ResultType")
ParameterType = TypeVar("ParameterType")


class QueryHandler(ABC, Generic[ParameterType, ResultType]):

    def __init__(self,
                 parameter: ParameterType,
                 query_handler_context: ScopeQueryHandlerContext):
        self._query_handler_context = query_handler_context

    @abstractmethod
    def start(self) -> Union[Continue, Finish[ResultType]]:
        raise NotImplementedError()

    @abstractmethod
    def handle_query_result(self, query_result: QueryResult) \
            -> Union[Continue, Finish[ResultType]]:
        raise NotImplementedError()
