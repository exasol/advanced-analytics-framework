from enum import Enum, auto

from exasol_advanced_analytics_framework.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext


class ReferenceCounterStatus(Enum):
    RELEASED = auto()
    NOT_RELEASED = auto()


class ObjectProxyReferenceCounter:
    """
    For the execution of the SQLStageGraph we have to keep track which SQLStage uses which temporary object.
    This is the reference counter for a temporary object (ObjectProxy). It counts how often you have added
    or removed the object. It contains scoped_query_context_handler corresponding to the ObjectProxies and
    calls release on it, when the counter gets 0. This releases the ObjectProxy.
    """

    def __init__(self, parent_query_context_handler: ScopeQueryHandlerContext, object_proxy: ObjectProxy):
        self._object_proxy = object_proxy
        self._valid = True
        self._parent_query_context_handler = parent_query_context_handler
        self._child_query_context_handler = self._parent_query_context_handler.get_child_query_handler_context()
        self._parent_query_context_handler.transfer_object_to(object_proxy, self._child_query_context_handler)
        self._counter = 1  # counter is one, because with zero this object wouldn't exist

    def _check_if_valid(self):
        if not self._valid:
            raise RuntimeError("ReferenceCounter not valid anymore. "
                               "ObjectProxy got already garbage collected or transfered back.")

    def add(self):
        self._check_if_valid()
        self._counter += 1

    def remove(self) -> ReferenceCounterStatus:
        self._check_if_valid()
        self._counter -= 1
        return self._release_if_not_used()

    def _release_if_not_used(self) -> ReferenceCounterStatus:
        if self._counter == 0:
            self._invalidate_and_release()
            return ReferenceCounterStatus.RELEASED
        else:
            return ReferenceCounterStatus.NOT_RELEASED

    def transfer_back_to_parent_query_handler_context(self):
        self._check_if_valid()
        self._child_query_context_handler.transfer_object_to(
            self._object_proxy, self._parent_query_context_handler)
        self._invalidate_and_release()

    def _invalidate_and_release(self):
        self._invalidate()  # We call first _invalidate, in case release throws an exception which gets caught
        self._child_query_context_handler.release()

    def _invalidate(self):
        self._valid = False
        self._counter = None
