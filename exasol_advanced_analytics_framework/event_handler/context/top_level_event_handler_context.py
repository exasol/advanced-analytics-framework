from abc import ABC
from typing import Set, List

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation
from exasol_data_science_utils_python.preprocessing.sql.schema.schema_name import SchemaName
from exasol_data_science_utils_python.preprocessing.sql.schema.table_name import TableName

from exasol_advanced_analytics_framework.event_handler.context.proxy.bucketfs_location_proxy import \
    BucketFSLocationProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.db_object_proxy import DBObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_proxy import TableProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.view_proxy import ViewProxy
from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class TemporaryObjectCounter:
    def __init__(self):
        self._counter = 0

    def get_current_value(self) -> int:
        result = self._counter
        self._counter += 1
        return result


class _ScopeEventHandlerContextBase(ScopeEventHandlerContext, ABC):
    def __init__(self,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 global_temporary_object_counter: TemporaryObjectCounter):
        self._global_temporary_object_counter = global_temporary_object_counter
        self._temporary_schema_name = temporary_schema_name
        self._temporary_bucketfs_location = temporary_bucketfs_location
        self._temporary_db_object_name_prefix = temporary_db_object_name_prefix
        self._valid_object_proxies: Set[ObjectProxy] = set()
        self._invalid_object_proxies: Set[ObjectProxy] = set()
        self._owned_object_proxies: Set[ObjectProxy] = set()
        self._counter = 0
        self._child_event_handler_context_list: List[_ChildEventHandlerContext] = []
        self._is_valid = True

    def release(self) -> None:
        self._check_if_valid()
        for object_proxy in list(self._owned_object_proxies):
            self._release_object(object_proxy)
        if len(self._valid_object_proxies) > 0:
            for object_proxy in list(self._valid_object_proxies):
                self._release_object(object_proxy)
            raise RuntimeError("Child contexts are not released.")
        self._invalidate()

    def _get_counter_value(self) -> int:
        self._check_if_valid()
        self._counter += 1
        return self._counter

    def _get_temporary_table_name(self) -> TableName:
        self._check_if_valid()
        temporary_name = self._get_temporary_db_object_name()
        temporary_table_name = TableName(table_name=temporary_name,
                                         schema=SchemaName(schema_name=self._temporary_schema_name))
        return temporary_table_name

    def _get_temporary_db_object_name(self) -> str:
        temporary_name = f"{self._temporary_db_object_name_prefix}_{self._get_counter_value()}"
        return temporary_name

    def _own_object(self, object_proxy: ObjectProxy):
        self._register_object(object_proxy)
        self._owned_object_proxies.add(object_proxy)

    def get_temporary_table(self) -> TableProxy:
        self._check_if_valid()
        temporary_table_name = self._get_temporary_table_name()
        object_proxy = TableProxy(temporary_table_name,
                                  self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_view(self) -> ViewProxy:
        self._check_if_valid()
        temporary_table_name = self._get_temporary_table_name()
        object_proxy = ViewProxy(temporary_table_name,
                                 self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_bucketfs_file(self) -> BucketFSLocationProxy:
        self._check_if_valid()
        temporary_path = self.get_temporary_path()
        child_bucketfs_location = self._temporary_bucketfs_location.joinpath(temporary_path)
        object_proxy = BucketFSLocationProxy(child_bucketfs_location)
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_path(self):
        temporary_path = f"{self._get_counter_value()}"
        return temporary_path

    def get_child_event_handler_context(self) -> ScopeEventHandlerContext:
        self._check_if_valid()
        temporary_path = self.get_temporary_path()
        new_temporary_bucketfs_location = self._temporary_bucketfs_location.joinpath(temporary_path)
        child_event_handler_conext = _ChildEventHandlerContext(
            self,
            new_temporary_bucketfs_location,
            self._get_temporary_db_object_name(),
            self._temporary_schema_name,
            self._global_temporary_object_counter
        )
        self._child_event_handler_context_list.append(child_event_handler_conext)
        return child_event_handler_conext

    def _is_child(self, scope_event_handler_context: ScopeEventHandlerContext) -> bool:
        result = isinstance(scope_event_handler_context, _ChildEventHandlerContext) and \
                 scope_event_handler_context._parent == self
        return result

    def _transfer_object_to(self, object_proxy: ObjectProxy,
                            scope_event_handler_conext: ScopeEventHandlerContext) -> None:
        self._check_if_valid()
        if object_proxy in self._owned_object_proxies:
            if isinstance(scope_event_handler_conext, _ScopeEventHandlerContextBase):
                scope_event_handler_conext._own_object(object_proxy)
                if not self._is_child(scope_event_handler_conext):
                    self._remove_object(object_proxy)
            else:
                raise ValueError(f"{scope_event_handler_conext.__class__} not allowed, "
                                 f"use a context created with get_child_event_handler_context")
        else:
            raise RuntimeError("Object not owned by this ScopeEventHandlerContext.")

    def _remove_object(self, object_proxy: ObjectProxy) -> None:
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
        child_context_were_not_released = False
        for child_event_handler_conext in self._child_event_handler_context_list:
            if child_event_handler_conext._is_valid:
                child_context_were_not_released = True
                child_event_handler_conext._invalidate()
        if child_context_were_not_released:
            raise RuntimeError("Child contexts are not released.")

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.add(object_proxy)

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_valid()
        self._valid_object_proxies.remove(object_proxy)
        if object_proxy in self._owned_object_proxies:
            self._owned_object_proxies.remove(object_proxy)
        self._invalid_object_proxies.add(object_proxy)


class TopLevelEventHandlerContext(_ScopeEventHandlerContextBase):
    def __init__(self,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 global_temporary_object_counter: TemporaryObjectCounter = TemporaryObjectCounter()):
        super().__init__(temporary_bucketfs_location,
                         temporary_db_object_name_prefix,
                         temporary_schema_name,
                         global_temporary_object_counter)

    def _release_object(self, object_proxy: ObjectProxy):
        super()._release_object(object_proxy)
        object_proxy._invalidate()

    def cleanup_released_object_proxies(self) -> List[Query]:
        """
        Cleans up released objects.
        BucketFSLocationProxies will be directly removed.
        For DBObjectProxies this method returns clean up queries.
        The clean up queries are sorted in reverse order of their creation,
        such that, we remove first objects that might depend on previous objects.
        """
        db_objects: List[DBObjectProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, DBObjectProxy)]
        bucketfs_objects: List[BucketFSLocationProxy] = \
            [object_proxy for object_proxy in self._invalid_object_proxies
             if isinstance(object_proxy, BucketFSLocationProxy)]
        self._invalid_object_proxies = set()
        self._remove_bucketfs_objects(bucketfs_objects)
        reverse_sorted_db_objects = sorted(db_objects, key=lambda x: x._global_counter_value, reverse=True)
        cleanup_queries = [object_proxy.get_cleanup_query()
                           for object_proxy in reverse_sorted_db_objects]
        return cleanup_queries

    @staticmethod
    def _remove_bucketfs_objects(bucketfs_object_proxies: List[BucketFSLocationProxy]):
        for object_proxy in bucketfs_object_proxies:
            object_proxy.cleanup()

    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_event_handler_context: ScopeEventHandlerContext):
        if self._is_child(scope_event_handler_context):
            self._transfer_object_to(object_proxy, scope_event_handler_context)
        else:
            raise RuntimeError("Given ScopeEventHandlerContext not a child.")


class _ChildEventHandlerContext(_ScopeEventHandlerContextBase):
    def __init__(self, parent: _ScopeEventHandlerContextBase,
                 temporary_bucketfs_location: AbstractBucketFSLocation,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 global_temporary_object_counter: TemporaryObjectCounter):
        super().__init__(temporary_bucketfs_location,
                         temporary_db_object_name_prefix,
                         temporary_schema_name,
                         global_temporary_object_counter)
        self.__parent = parent

    @property
    def _parent(self) -> _ScopeEventHandlerContextBase:
        return self.__parent

    def _release_object(self, object_proxy: ObjectProxy):
        super()._release_object(object_proxy)
        self._parent._release_object(object_proxy)

    def _register_object(self, object_proxy: ObjectProxy):
        super()._register_object(object_proxy)
        self._parent._register_object(object_proxy)

    def _is_parent(self, scope_event_handler_context: ScopeEventHandlerContext) -> bool:
        result = self._parent == scope_event_handler_context
        return result

    def _is_sibling(self, scope_event_handler_context: ScopeEventHandlerContext) -> bool:
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
