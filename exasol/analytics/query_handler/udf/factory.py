from abc import ABC, abstractmethod

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.udf.interface import UDFQueryHandler


class UDFQueryHandlerFactory(ABC):
    """
    An abstract class for factories which are injected by name to the QueryHandlerRunnerUDF
    which then will create the instance from the name.
    """

    @abstractmethod
    def create(
        self, parameter: str, query_handler_context: ScopeQueryHandlerContext
    ) -> UDFQueryHandler:
        """Creates a UDFQueryHandler"""
