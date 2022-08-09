from abc import abstractmethod

from exasol_advanced_analytics_framework.event_handler.context.event_handler_context import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy


class ScopeEventHandlerContext(EventHandlerContext):
    @abstractmethod
    def release(self):
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
        pass
