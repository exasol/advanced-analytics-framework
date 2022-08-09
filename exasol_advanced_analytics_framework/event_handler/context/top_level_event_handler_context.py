from abc import ABC
from typing import Set, List

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.proxy.bucketfs_location_proxy import \
    BucketFSLocationProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.db_object_proxy import DBObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_proxy import TableProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.view_proxy import ViewProxy
from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class _ScopeEventHandlerContextBase(ScopeEventHandlerContext, ABC):
    def __init__(self,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_name_prefix: str):
        self._temporary_bucketfs_location = temporary_bucketfs_location
        self._temporary_name_prefix = temporary_name_prefix
        self._valid_object_proxies: Set[ObjectProxy] = set()
        self._invalid_object_proxies: Set[ObjectProxy] = set()
        self._owned_object_proxies: Set[ObjectProxy] = set()
        self._counter = 0
        self._child_event_handler_context_list: List[_ChildEventHandlerContext] = []
        self._is_valid = True

    def release(self):
        self._check_if_valid()
        for object_proxy in list(self._valid_object_proxies):
            self._release_object(object_proxy)
        self._invalidate()

    def _get_temporary_name(self) -> str:
        self._check_if_valid()
        self._counter += 1
        return f"{self._temporary_name_prefix}_{self._counter}"

    def _own_object(self, object_proxy: ObjectProxy):
        self._register_object(object_proxy)
        self._owned_object_proxies.add(object_proxy)

    def get_temporary_table(self) -> TableProxy:
        self._check_if_valid()
        temporary_name = self._get_temporary_name()
        object_proxy = TableProxy(temporary_name)
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_view(self) -> ViewProxy:
        self._check_if_valid()
        temporary_name = self._get_temporary_name()
        object_proxy = ViewProxy(temporary_name)
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_bucketfs_file(self) -> BucketFSLocationProxy:
        self._check_if_valid()
        temporary_path = self._get_temporary_name()
        child_bucketfs_location = self._temporary_bucketfs_location.joinpath(temporary_path)
        object_proxy = BucketFSLocationProxy(child_bucketfs_location)
        self._own_object(object_proxy)
        return object_proxy

    def get_child_event_handler_context(self) -> ScopeEventHandlerContext:
        self._check_if_valid()
        child_event_handler_conext = _ChildEventHandlerContext(
            self,
            self._temporary_bucketfs_location,
            self._get_temporary_name())
        self._child_event_handler_context_list.append(child_event_handler_conext)
        return child_event_handler_conext

    def _is_child(self, scope_event_handler_context: ScopeEventHandlerContext):
        result = isinstance(scope_event_handler_context, _ChildEventHandlerContext) and \
                 scope_event_handler_context._parent == self
        return result

    def _transfer_object_to(self, object_proxy: ObjectProxy,
                            scope_event_handler_conext: ScopeEventHandlerContext):
        self._check_if_valid()
        if object_proxy in self._owned_object_proxies:
            scope_event_handler_conext._own_object(object_proxy)
            if not self._is_child(scope_event_handler_conext):
                self._remove_object(object_proxy)
        else:
            raise RuntimeError("Object not owned by this ScopeEventHandlerContext.")

    def _remove_object(self, object_proxy: ObjectProxy):
        self._valid_object_proxies.remove(object_proxy)
        self._owned_object_proxies.remove(object_proxy)

    def _check_if_valid(self):
        if not self._is_valid:
            raise RuntimeError("Context already released.")

    def _invalidate(self):
        self._check_if_valid()
        self._invalid_object_proxies = self._invalid_object_proxies.union(self._valid_object_proxies)
        self._valid_object_proxies = set()
        self._owned_object_proxies = set()
        self._is_valid = False
        for child_event_handler_conext in self._child_event_handler_context_list:
            child_event_handler_conext._invalidate()


class TopLevelEventHandlerContext(_ScopeEventHandlerContextBase):
    def __init__(self,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_name_prefix: str = None):
        super().__init__(temporary_bucketfs_location, temporary_name_prefix)

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.remove(object_proxy)
        if object_proxy in self._owned_object_proxies:
            self._owned_object_proxies.remove(object_proxy)
        self._invalid_object_proxies.add(object_proxy)
        object_proxy._invalidate()

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.add(object_proxy)

    def cleanup_invalid_object_proxies(self) -> List[Query]:
        db_objects: List[DBObjectProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, DBObjectProxy)]
        bucketfs_objects: List[BucketFSLocationProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, BucketFSLocationProxy)]
        self._invalid_object_proxies = set()
        self._remove_bucketfs_objects(bucketfs_objects)
        return [object_proxy.get_cleanup_query() for object_proxy in db_objects]

    def _remove_bucketfs_objects(self, bucketfs_object_proxies: List[BucketFSLocationProxy]):
        for object_proxy in bucketfs_object_proxies:
            object_proxy.cleanup()

    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_event_handler_context: ScopeEventHandlerContext):
        if self._is_child(scope_event_handler_context):
            self._transfer_object_to(object_proxy, scope_event_handler_context)
        else:
            raise RuntimeError("Given ScopeEventHandlerContext not a child.")


class _ChildEventHandlerContext(_ScopeEventHandlerContextBase):
    def __init__(self, parent: ScopeEventHandlerContext,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_name_prefix: str = None):
        super().__init__(temporary_bucketfs_location, temporary_name_prefix)
        self.__parent = parent

    @property
    def _parent(self) -> ScopeEventHandlerContext:
        return self.__parent

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.remove(object_proxy)
        self._invalid_object_proxies.add(object_proxy)
        self._parent._release_object(object_proxy)

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.add(object_proxy)
        self._parent._register_object(object_proxy)

    def _is_parent(self, scope_event_handler_context: ScopeEventHandlerContext):
        result = self._parent == scope_event_handler_context
        return result

    def _is_sibling(self, scope_event_handler_context: ScopeEventHandlerContext):
        result = isinstance(scope_event_handler_context, _ChildEventHandlerContext) and \
                 scope_event_handler_context._parent == self._parent
        return result

    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_event_handler_context: ScopeEventHandlerContext):
        if self._is_child(scope_event_handler_context) or \
                self._is_parent(scope_event_handler_context) or \
                self._is_sibling(scope_event_handler_context):
            self._transfer_object_to(object_proxy, scope_event_handler_context)
        else:
            raise RuntimeError("Given ScopeEventHandlerContext not a child, parent or sibling.")
