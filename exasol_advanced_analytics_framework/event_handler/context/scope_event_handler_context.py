from abc import abstractmethod

from exasol_advanced_analytics_framework.event_handler.context.event_handler_context import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy


class ScopeEventHandlerContext(EventHandlerContext):
    @abstractmethod
    def release(self):
        """
        This function release all temporary objects registered with this context or any of its descendants.
        However, it throws also an exception when you didn't release the children's.
        """
        pass

    @abstractmethod
    def get_child_event_handler_context(self) -> "ScopeEventHandlerContext":
        pass

    @abstractmethod
    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_event_handler_context: "ScopeEventHandlerContext"):
        """
        This function transfers the ownership of the object to a different context.
        That means, that the object isn't released if this context is released,
        instead it will be released if the other context, it was transferred to, is released.
        However, the object can be only transferred to the parent, child or sibling context.
        The first owner is always the context where one of the get_*_object function was called.
        Transferring the object from one context to another can be used in conjunction with
        nested event handlers where you want to cleanup after one event handler finished,
        but want to exchange some temporary objects between the event handlers. The parent event 
        handler is always responsible for the transfer.
        """
        pass
