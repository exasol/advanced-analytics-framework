from abc import abstractmethod

from exasol_advanced_analytics_framework.event_handler.context.event_handler_context import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy


class ScopeEventHandlerContext(EventHandlerContext):
    @abstractmethod
    def release(self):
        """
        This function release all temporary objects registered with context or any of it descendants.
        """
        pass

    @abstractmethod
    def get_child_event_handler_context(self) -> "ChildEventHandlerContext":
        pass

    @abstractmethod
    def _release_object(self, object_proxy: ObjectProxy):
        pass

    @abstractmethod
    def _register_object(self, object_proxy: ObjectProxy):
        pass

    @abstractmethod
    def _own_object(self, object_proxy: ObjectProxy):
        pass

    @abstractmethod
    def _invalidate(self):
        pass

    @abstractmethod
    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_event_handler_context: "ScopeEventHandlerContext"):
        """
        This function transfers the ownershio if the object to a different context.
        That means, that the object isn't released if this context is released,
        instread it will be released if the ohter context is released.
        However, object can be only transferred to the parent, child or sibling context.
        The first owner is always the context where one of the get_*_object function was called.
        """
        pass
