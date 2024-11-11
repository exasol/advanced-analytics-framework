import textwrap
import traceback
from abc import ABC
from typing import Set, List, Callable

import exasol.bucketfs as bfs
from exasol.analytics.schema import (
    SchemaName,
    TableNameBuilder,
    UDFNameBuilder,
    UDFName,
    ViewNameBuilder,
    TableName,
    ViewName,
)

from exasol.analytics.query.handler.context.connection_name_proxy import ConnectionNameProxy
from exasol.analytics.query.handler.context.connection_name import ConnectionName, ConnectionNameImpl
from exasol.analytics.query.handler.context.proxy.bucketfs_location_proxy import     BucketFSLocationProxy
from exasol.analytics.query.handler.context.proxy.db_object_name_proxy import DBObjectNameProxy
from exasol.analytics.query.handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query.handler.context.proxy.table_name_proxy import TableNameProxy
from exasol.analytics.query.handler.context.proxy.udf_name_proxy import UDFNameProxy
from exasol.analytics.query.handler.context.proxy.view_name_proxy import ViewNameProxy
from exasol.analytics.query.handler.context.scope import     ScopeQueryHandlerContext, Connection
from exasol.analytics.query.handler.query.interface import Query


class TemporaryObjectCounter:
    def __init__(self):
        self._counter = 0

    def get_current_value(self) -> int:
        result = self._counter
        self._counter += 1
        return result


class ChildContextNotReleasedError(Exception):

    def __init__(self,
                 not_released_child_contexts: List[ScopeQueryHandlerContext],
                 exceptions_thrown_by_not_released_child_contexts: List["ChildContextNotReleasedError"]):
        """
        :param not_released_child_contexts: A list of child contexts which were not yet released
        :param exceptions_thrown_by_not_released_child_contexts: A list of ChildContextNotReleasedError thrown by the
                                                                 call to _invalidate of the child contexts
        """
        self.exceptions_thrown_by_not_released_child_contexts = exceptions_thrown_by_not_released_child_contexts
        self.not_released_child_contexts = not_released_child_contexts
        concatenated_contexts = "\n- ".join([str(c) for c in self.get_all_not_released_contexts()])
        self.message = \
            f"The following child contexts were not released,\n" \
            f"please release all contexts to avoid ressource leakage:\n" \
            f"- {concatenated_contexts}\n"
        super(ChildContextNotReleasedError, self).__init__(self.message)

    def get_all_not_released_contexts(self):
        result = sum([e.get_all_not_released_contexts() for e in
                      self.exceptions_thrown_by_not_released_child_contexts], [])
        result = self.not_released_child_contexts + result
        return result


ConnectionLookup = Callable[[str], Connection]


class _ScopeQueryHandlerContextBase(ScopeQueryHandlerContext, ABC):
    def __init__(self,
                 temporary_bucketfs_location: bfs.path.PathLike,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 connection_lookup: ConnectionLookup,
                 global_temporary_object_counter: TemporaryObjectCounter):
        self._connection_lookup = connection_lookup
        self._global_temporary_object_counter = global_temporary_object_counter
        self._temporary_schema_name = temporary_schema_name
        self._temporary_bucketfs_location = temporary_bucketfs_location
        self._temporary_db_object_name_prefix = temporary_db_object_name_prefix
        self._not_released_object_proxies: Set[ObjectProxy] = set()
        self._released_object_proxies: Set[ObjectProxy] = set()
        self._owned_object_proxies: Set[ObjectProxy] = set()
        self._counter = 0
        self._child_query_handler_context_list: List[_ChildQueryHandlerContext] = []
        self._not_released = True

    def release(self) -> None:
        self._check_if_released()
        for object_proxy in list(self._owned_object_proxies):
            self._release_object(object_proxy)
        if len(self._not_released_object_proxies) > 0:
            for object_proxy in list(self._not_released_object_proxies):
                self._release_object(object_proxy)
        self._release()
        self._check_if_children_released()

    def _get_counter_value(self) -> int:
        self._check_if_released()
        self._counter += 1
        return self._counter

    def _get_temporary_table_name(self) -> TableName:
        self._check_if_released()
        temporary_name = self._get_temporary_db_object_name()
        temporary_table_name = TableNameBuilder.create(
            name=temporary_name,
            schema=SchemaName(schema_name=self._temporary_schema_name))
        return temporary_table_name

    def _get_temporary_view_name(self) -> ViewName:
        self._check_if_released()
        temporary_name = self._get_temporary_db_object_name()
        temporary_view_name = ViewNameBuilder.create(
            name=temporary_name,
            schema=SchemaName(schema_name=self._temporary_schema_name))
        return temporary_view_name

    def _get_temporary_udf_name(self) -> UDFName:
        self._check_if_released()
        temporary_name = self._get_temporary_db_object_name()
        temporary_script_name = UDFNameBuilder.create(
            name=temporary_name,
            schema=SchemaName(schema_name=self._temporary_schema_name))
        return temporary_script_name

    def _get_temporary_connection_name(self) -> ConnectionName:
        self._check_if_released()
        temporary_name = self._get_temporary_db_object_name()
        temporary_connection_name = ConnectionNameImpl(connection_name=temporary_name)
        return temporary_connection_name

    def _get_temporary_db_object_name(self) -> str:
        temporary_name = f"{self._temporary_db_object_name_prefix}_{self._get_counter_value()}"
        return temporary_name

    def _own_object(self, object_proxy: ObjectProxy):
        self._register_object(object_proxy)
        self._owned_object_proxies.add(object_proxy)

    def get_temporary_name(self) -> str:
        self._check_if_released()
        temporary_name = self._get_temporary_db_object_name()
        return temporary_name

    def get_temporary_table_name(self) -> TableName:
        self._check_if_released()
        temporary_table_name = self._get_temporary_table_name()
        object_proxy = TableNameProxy(temporary_table_name,
                                      self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_view_name(self) -> ViewName:
        self._check_if_released()
        temporary_view_name = self._get_temporary_view_name()
        object_proxy = ViewNameProxy(temporary_view_name,
                                     self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_udf_name(self) -> UDFName:
        self._check_if_released()
        temporary_script_name = self._get_temporary_udf_name()
        object_proxy = UDFNameProxy(temporary_script_name,
                                    self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_connection_name(self) -> ConnectionName:
        self._check_if_released()
        temporary_connection_name = self._get_temporary_connection_name()
        object_proxy = ConnectionNameProxy(connection_name=temporary_connection_name,
                                           global_counter_value=self._global_temporary_object_counter.get_current_value())
        self._own_object(object_proxy)
        return object_proxy

    def get_temporary_bucketfs_location(self) -> BucketFSLocationProxy:
        self._check_if_released()
        temporary_path = self._get_temporary_path()
        child_bucketfs_location = self._temporary_bucketfs_location.joinpath(temporary_path)
        object_proxy = BucketFSLocationProxy(child_bucketfs_location)
        self._own_object(object_proxy)
        return object_proxy

    def _get_temporary_path(self):
        temporary_path = f"{self._get_counter_value()}"
        return temporary_path

    def get_child_query_handler_context(self) -> ScopeQueryHandlerContext:
        self._check_if_released()
        temporary_path = self._get_temporary_path()
        new_temporary_bucketfs_location = self._temporary_bucketfs_location.joinpath(temporary_path)
        child_query_handler_context = _ChildQueryHandlerContext(
            self,
            new_temporary_bucketfs_location,
            self._get_temporary_db_object_name(),
            self._temporary_schema_name,
            self._connection_lookup,
            self._global_temporary_object_counter
        )
        self._child_query_handler_context_list.append(child_query_handler_context)
        return child_query_handler_context

    def _is_child(self, scope_query_handler_context: ScopeQueryHandlerContext) -> bool:
        result = isinstance(scope_query_handler_context, _ChildQueryHandlerContext) and \
                 scope_query_handler_context._parent == self
        return result

    def _transfer_object_to(self, object_proxy: ObjectProxy,
                            scope_query_handler_context: ScopeQueryHandlerContext) -> None:
        self._check_if_released()
        if object_proxy in self._owned_object_proxies:
            if isinstance(scope_query_handler_context, _ScopeQueryHandlerContextBase):
                scope_query_handler_context._own_object(object_proxy)
                self._un_own_object(object_proxy)
                if not self._is_child(scope_query_handler_context):
                    self._remove_object(object_proxy)
            else:
                raise ValueError(f"{scope_query_handler_context.__class__} not allowed, "
                                 f"use a context created with get_child_query_handler_context")
        else:
            raise RuntimeError("Object not owned by this ScopeQueryHandlerContext.")

    def _remove_object(self, object_proxy: ObjectProxy) -> None:
        self._not_released_object_proxies.remove(object_proxy)

    def _un_own_object(self, object_proxy: ObjectProxy) -> None:
        self._owned_object_proxies.remove(object_proxy)

    def _check_if_released(self):
        if not self._not_released:
            raise RuntimeError("Context already released.")

    def _release(self):
        self._check_if_released()
        self._released_object_proxies = self._released_object_proxies.union(self._not_released_object_proxies)
        self._not_released_object_proxies = set()
        self._owned_object_proxies = set()
        self._not_released = False

    def _check_if_children_released(self):
        not_released_child_contexts = []
        exceptions_from_not_released_child_contexts = []
        for child_query_handler_context in self._child_query_handler_context_list:
            if child_query_handler_context._not_released:
                not_released_child_contexts.append(child_query_handler_context)
                try:
                    child_query_handler_context._release()
                    child_query_handler_context._check_if_children_released()
                except ChildContextNotReleasedError as e:
                    exceptions_from_not_released_child_contexts.append(e)
        if not_released_child_contexts:
            raise ChildContextNotReleasedError(
                not_released_child_contexts=not_released_child_contexts,
                exceptions_thrown_by_not_released_child_contexts=exceptions_from_not_released_child_contexts
            )

    def _register_object(self, object_proxy: ObjectProxy):
        self._check_if_released()
        self._not_released_object_proxies.add(object_proxy)

    def _release_object(self, object_proxy: ObjectProxy):
        self._check_if_released()
        self._not_released_object_proxies.remove(object_proxy)
        if object_proxy in self._owned_object_proxies:
            self._owned_object_proxies.remove(object_proxy)
        self._released_object_proxies.add(object_proxy)

    def get_connection(self, name: str) -> Connection:
        return self._connection_lookup(name)


class TopLevelQueryHandlerContext(_ScopeQueryHandlerContextBase):
    def __init__(self,
                 temporary_bucketfs_location: bfs.path.PathLike,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 connection_lookup: ConnectionLookup,
                 global_temporary_object_counter: TemporaryObjectCounter = TemporaryObjectCounter()):
        super().__init__(temporary_bucketfs_location,
                         temporary_db_object_name_prefix,
                         temporary_schema_name,
                         connection_lookup,
                         global_temporary_object_counter)

    def _release_object(self, object_proxy: ObjectProxy):
        super()._release_object(object_proxy)
        object_proxy._release()

    def cleanup_released_object_proxies(self) -> List[Query]:
        """
        Cleans up released objects.
        BucketFSLocationProxies will be directly removed.
        For DBObjectProxies this method returns clean up queries.
        The clean up queries are sorted in reverse order of their creation,
        such that, we remove first objects that might depend on previous objects.
        """
        db_objects: List[DBObjectNameProxy] = \
            [object_proxy for object_proxy in self._released_object_proxies
             if isinstance(object_proxy, DBObjectNameProxy)]
        bucketfs_objects: List[BucketFSLocationProxy] = \
            [object_proxy for object_proxy in self._released_object_proxies
             if isinstance(object_proxy, BucketFSLocationProxy)]
        self._released_object_proxies = set()
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
                           scope_query_handler_context: ScopeQueryHandlerContext):
        if self._is_child(scope_query_handler_context):
            self._transfer_object_to(object_proxy, scope_query_handler_context)
        else:
            raise RuntimeError("Given ScopeQueryHandlerContext not a child.")


class _ChildQueryHandlerContext(_ScopeQueryHandlerContextBase):
    def __init__(self, parent: _ScopeQueryHandlerContextBase,
                 temporary_bucketfs_location: bfs.path.PathLike,
                 temporary_db_object_name_prefix: str,
                 temporary_schema_name: str,
                 connection_lookup: ConnectionLookup,
                 global_temporary_object_counter: TemporaryObjectCounter):
        super().__init__(temporary_bucketfs_location,
                         temporary_db_object_name_prefix,
                         temporary_schema_name,
                         connection_lookup,
                         global_temporary_object_counter)
        self.__parent = parent

    @property
    def _parent(self) -> _ScopeQueryHandlerContextBase:
        return self.__parent

    def _release_object(self, object_proxy: ObjectProxy):
        super()._release_object(object_proxy)
        self._parent._release_object(object_proxy)

    def _register_object(self, object_proxy: ObjectProxy):
        super()._register_object(object_proxy)
        self._parent._register_object(object_proxy)

    def _is_parent(self, scope_query_handler_context: ScopeQueryHandlerContext) -> bool:
        result = self._parent == scope_query_handler_context
        return result

    def _is_sibling(self, scope_query_handler_context: ScopeQueryHandlerContext) -> bool:
        result = isinstance(scope_query_handler_context, _ChildQueryHandlerContext) and \
                 scope_query_handler_context._parent == self._parent
        return result

    def transfer_object_to(self, object_proxy: ObjectProxy,
                           scope_query_handler_context: ScopeQueryHandlerContext):
        if self._is_child(scope_query_handler_context) or \
                self._is_parent(scope_query_handler_context) or \
                self._is_sibling(scope_query_handler_context):
            self._transfer_object_to(object_proxy, scope_query_handler_context)
        else:
            raise RuntimeError("Given ScopeQueryHandlerContext not a child, parent or sibling.")
