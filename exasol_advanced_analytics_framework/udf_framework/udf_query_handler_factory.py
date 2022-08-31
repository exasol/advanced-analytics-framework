from abc import ABC, abstractmethod

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler


class UDFQueryHandlerFactory(ABC):
    """
    An abstract class for factories which are injected by name to the QueryHandlerRunnerUDF
    which then will create the instance from the name.
    """

    @abstractmethod
    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        """Creates a UDFQueryHandler"""
