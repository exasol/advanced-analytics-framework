import enum
from abc import ABC, abstractmethod

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query_handler.context.query_handler_context import (
    QueryHandlerContext,
)


class Connection(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the connection object"""

    @property
    @abstractmethod
    def address(self) -> str:
        """Address of the connection object"""

    @property
    @abstractmethod
    def user(self) -> str:
        """User of the connection object"""

    @property
    @abstractmethod
    def password(self) -> str:
        """Password of the connection object"""


class ScopeQueryHandlerContext(QueryHandlerContext):
    @abstractmethod
    def release(self):
        """
        This function release all temporary objects registered with this context or any of its descendants.
        However, it throws also an exception when you didn't release the children's.
        """
        pass

    @abstractmethod
    def get_child_query_handler_context(self) -> "ScopeQueryHandlerContext":
        pass

    @abstractmethod
    def transfer_object_to(
        self,
        object_proxy: ObjectProxy,
        scope_query_handler_context: "ScopeQueryHandlerContext",
    ):
        """
        This function transfers the ownership of the object to a different context.
        That means, that the object isn't released if this context is released,
        instead it will be released if the other context, it was transferred to, is released.
        However, the object can be only transferred to the parent, child or sibling context.
        The first owner is always the context where one of the get_*_object function was called.
        Transferring the object from one context to another can be used in conjunction with
        nested query handlers where you want to cleanup after one query handler finished,
        but want to exchange some temporary objects between the query handlers. The parent query
        handler is always responsible for the transfer.
        """
        pass

    @abstractmethod
    def get_connection(self, name: str) -> Connection:
        pass
