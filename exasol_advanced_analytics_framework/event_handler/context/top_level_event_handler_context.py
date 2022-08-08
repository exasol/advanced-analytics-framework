from abc import ABC
from typing import Set, List

from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.proxy.bucketfs_file_proxy import BucketFSFileProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.db_object_proxy import DBObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_proxy import TableProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.view_proxy import ViewProxy
from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext, ChildEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class _ScopeEventHandlerContextBase(ScopeEventHandlerContext, ABC):
    def __init__(self,
                 temporary_bucketfs_location: BucketFSLocation,
                 temporary_name_prefix: str):
        self._temporary_bucketfs_location = temporary_bucketfs_location
        self._temporary_name_prefix = temporary_name_prefix
        self._valid_object_proxies: Set[ObjectProxy] = set()
        self._invalid_object_proxies: Set[ObjectProxy] = set()
        self._counter = 0
        self._sub_event_handler_conext_list: List[ChildEventHandlerContext] = []
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

    def get_temporary_table(self) -> TableProxy:
        self._check_if_valid()
        temporary_name = self._get_temporary_name()
        object_proxy = TableProxy(temporary_name)
        self._register_object(object_proxy)
        return object_proxy

    def get_temporary_view(self) -> ViewProxy:
        self._check_if_valid()
        temporary_name = self._get_temporary_name()
        object_proxy = ViewProxy(temporary_name)
        self._register_object(object_proxy)
        return object_proxy

    def get_temporary_bucketfs_file(self) -> BucketFSFileProxy:
        self._check_if_valid()
        raise NotImplementedError()

    def get_child_event_handler_context(self) -> ChildEventHandlerContext:
        self._check_if_valid()
        sub_event_handler_conext = _ChildEventHandlerContext(
            self,
            self._temporary_bucketfs_location,
            self._get_temporary_name())
        self._sub_event_handler_conext_list.append(sub_event_handler_conext)
        return sub_event_handler_conext

    def _check_if_valid(self):
        if not self._is_valid:
            raise RuntimeError("Not valid")

    def _invalidate(self):
        self._check_if_valid()
        self._invalid_object_proxies = self._invalid_object_proxies.union(self._valid_object_proxies)
        self._valid_object_proxies = set()
        self._is_valid = False
        for sub_event_handler_conext in self._sub_event_handler_conext_list:
            sub_event_handler_conext._invalidate()


class TopLevelEventHandlerContext(_ScopeEventHandlerContextBase):
    def __init__(self,
                 temporary_bucketfs_location: BucketFSLocation,
                 temporary_name_prefix: str = None):
        super().__init__(temporary_bucketfs_location, temporary_name_prefix)

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.remove(object_proxy)
        self._invalid_object_proxies.add(object_proxy)
        object_proxy._invalidate()

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.add(object_proxy)

    def cleanup_invalid_object_proxies(self) -> List[Query]:
        db_objects: List[DBObjectProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, DBObjectProxy)]
        bucketfs_objects: List[BucketFSFileProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, BucketFSFileProxy)]
        self._invalid_object_proxies = set()
        self._remove_bucketfs_objects(bucketfs_objects)
        return [object_proxy.get_cleanup_query() for object_proxy in db_objects]

    def _remove_bucketfs_objects(self, bucketfs_objects: List[BucketFSFileProxy]):
        pass


class _ChildEventHandlerContext(_ScopeEventHandlerContextBase, ChildEventHandlerContext):
    def __init__(self, parent: ScopeEventHandlerContext,
                 temporary_bucketfs_location: BucketFSLocation,
                 temporary_name_prefix: str = None):
        super().__init__(temporary_bucketfs_location, temporary_name_prefix)
        self.__parent = parent

    @property
    def _parent(self) -> ScopeEventHandlerContext:
        return self.__parent

    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scoped_event_handler_conext: "ChildEventHandlerContext"):
        self._check_if_valid()
        if self._parent != scoped_event_handler_conext._parent:
            raise RuntimeError("Not the same lavel")
        if object_proxy in self._valid_object_proxies:
            scoped_event_handler_conext._register_object(object_proxy)
            self._valid_object_proxies.remove(object_proxy)
        else:
            raise RuntimeError("Object not valid or not registered")

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.remove(object_proxy)
        self._invalid_object_proxies.add(object_proxy)
        self._parent._release_object(object_proxy)

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.add(object_proxy)
        self._parent._register_object(object_proxy)
