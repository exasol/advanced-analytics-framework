from typing import (
    Callable,
    Dict,
)

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counter import (
    ObjectProxyReferenceCounter,
    ReferenceCounterStatus,
)

ObjectProxyReferenceCounterFactory = Callable[
    [ScopeQueryHandlerContext, ObjectProxy], ObjectProxyReferenceCounter
]


class ObjectProxyReferenceCountingBag:
    """
    A Bag used to keep track of the usage of temporary ObjectProxy objects.
    It uses a reference counter for each ObjectProxy. If the reference counter
    reaches zero the corresponding ScopeQueryHandlerContext gets released.
    """

    def __init__(
        self,
        parent_query_context_handler: ScopeQueryHandlerContext,
        object_proxy_reference_counter_factory: ObjectProxyReferenceCounterFactory = ObjectProxyReferenceCounter,
    ):
        self._object_proxy_reference_counter_factory = (
            object_proxy_reference_counter_factory
        )
        self._parent_query_context_handler = parent_query_context_handler
        self._reference_counter_map: Dict[ObjectProxy, ObjectProxyReferenceCounter] = {}

    def add(self, object_proxy: ObjectProxy):
        if object_proxy not in self._reference_counter_map:
            self._reference_counter_map[object_proxy] = (
                self._object_proxy_reference_counter_factory(
                    self._parent_query_context_handler, object_proxy
                )
            )
        else:
            self._reference_counter_map[object_proxy].add()

    def remove(self, object_proxy: ObjectProxy):
        reference_counter_status = self._reference_counter_map[object_proxy].remove()
        if reference_counter_status == ReferenceCounterStatus.RELEASED:
            del self._reference_counter_map[object_proxy]

    def __contains__(self, item: ObjectProxy):
        result = item in self._reference_counter_map
        return result

    def transfer_back_to_parent_query_handler_context(self, object_proxy: ObjectProxy):
        self._reference_counter_map[
            object_proxy
        ].transfer_back_to_parent_query_handler_context()
        del self._reference_counter_map[object_proxy]
