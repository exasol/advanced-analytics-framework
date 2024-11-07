from contextlib import contextmanager

import pytest
import exasol.bucketfs as bfs
from exasol.analytics.schema import (
    SchemaName,
    ColumnBuilder,
    ColumnName,
    UDFName,
    ColumnType,
    View,
    Table,
)

from exasol.analytics.query_handler.context.connection_name import ConnectionName
from exasol.analytics.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext, Connection
from exasol.analytics.query_handler.context.top_level_query_handler_context import \
    ChildContextNotReleasedError


def test_temporary_table_prefix_in_name(scope_query_handler_context: ScopeQueryHandlerContext,
                                        prefix: str):
    proxy = scope_query_handler_context.get_temporary_table_name()
    assert proxy.name.startswith(prefix)


def test_temporary_table_temporary_schema(scope_query_handler_context: ScopeQueryHandlerContext,
                                          schema: str):
    proxy = scope_query_handler_context.get_temporary_table_name()
    assert proxy.schema_name.name == schema


def test_temporary_view_prefix_in_name(scope_query_handler_context: ScopeQueryHandlerContext,
                                       prefix: str):
    proxy = scope_query_handler_context.get_temporary_view_name()
    assert proxy.name.startswith(prefix)


def test_temporary_view_temporary_schema(scope_query_handler_context: ScopeQueryHandlerContext,
                                         schema: str):
    proxy = scope_query_handler_context.get_temporary_view_name()
    assert proxy.schema_name.name == schema


def test_temporary_connection_temporary(scope_query_handler_context: ScopeQueryHandlerContext,
                                        schema: str):
    proxy = scope_query_handler_context.get_temporary_connection_name()
    assert isinstance(proxy, ConnectionName)


def test_temporary_udf_temporary(
        scope_query_handler_context: ScopeQueryHandlerContext,
        schema: str):
    proxy = scope_query_handler_context.get_temporary_udf_name()
    assert isinstance(proxy, UDFName) and \
        proxy.schema_name == SchemaName(schema)


def test_temporary_bucketfs_file_prefix_in_name(bucketfs_location: bfs.path.PathLike,
                                                scope_query_handler_context: ScopeQueryHandlerContext):
    proxy =  scope_query_handler_context.get_temporary_bucketfs_location()
    actual_path = proxy.bucketfs_location().as_udf_path()
    expected_prefix_path = bucketfs_location.as_udf_path()
    assert actual_path.startswith(expected_prefix_path)


def test_two_temporary_table_are_not_equal(scope_query_handler_context: ScopeQueryHandlerContext):
    proxy1 = scope_query_handler_context.get_temporary_table_name()
    proxy2 = scope_query_handler_context.get_temporary_table_name()
    assert proxy1.name != proxy2.name


def test_two_temporary_view_are_not_equal(scope_query_handler_context: ScopeQueryHandlerContext):
    proxy1 = scope_query_handler_context.get_temporary_view_name()
    proxy2 = scope_query_handler_context.get_temporary_view_name()
    assert proxy1.name != proxy2.name


def test_two_temporary_bucketfs_files_are_not_equal(scope_query_handler_context: ScopeQueryHandlerContext):
    proxy1 = scope_query_handler_context.get_temporary_bucketfs_location()
    proxy2 = scope_query_handler_context.get_temporary_bucketfs_location()
    path1 = proxy1.bucketfs_location().as_udf_path()
    path2 = proxy2.bucketfs_location().as_udf_path()
    assert path1 != path2


def test_temporary_table_name_proxy_use_name_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_table_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.name


def test_temporary_view_name_proxy_use_name_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_view_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.name


def test_temporary_table_name_proxy_use_schema_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_table_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.schema_name


def test_temporary_view_name_proxy_use_schema_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_view_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.schema_name


def test_temporary_table_name_proxy_use_quoted_name_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_table_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.quoted_name


def test_temporary_view_name_proxy_use_quoted_name_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_view_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.quoted_name


def test_temporary_table_name_proxy_use_fully_qualified_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_table_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.fully_qualified


def test_temporary_view_name_proxy_use_fully_qualified_after_release_fails(
        scope_query_handler_context: ScopeQueryHandlerContext):
    proxy = scope_query_handler_context.get_temporary_view_name()
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.fully_qualified


def test_get_temporary_view_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_query_handler_context.get_temporary_view_name()


def test_get_temporary_table_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_query_handler_context.get_temporary_table_name()


def test_get_temporary_bucketfs_file_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    scope_query_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_query_handler_context.get_temporary_bucketfs_location()


def test_use_child_context_after_release_fails(scope_query_handler_context: ScopeQueryHandlerContext):
    child = scope_query_handler_context.get_child_query_handler_context()
    try:
        scope_query_handler_context.release()
    except:
        pass
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = child.get_temporary_view_name()


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))


def test_transfer_between_siblings(scope_query_handler_context: ScopeQueryHandlerContext):
    child1 = scope_query_handler_context.get_child_query_handler_context()
    child2 = scope_query_handler_context.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    object_proxy2 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, child2)
    child1.release()

    with not_raises(Exception):
        _2 = object_proxy1.name
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        _ = object_proxy2.name


def test_transfer_siblings_check_ownership_transfer_to_target(scope_query_handler_context: ScopeQueryHandlerContext):
    child1 = scope_query_handler_context.get_child_query_handler_context()
    child2 = scope_query_handler_context.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    object_proxy2 = child2.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, child2)
    child2.transfer_object_to(object_proxy1, child1)
    child2.release()

    with not_raises(Exception):
        _ = object_proxy1.name
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        _ = object_proxy2.name


def test_transfer_child_parent_check_ownership_transfer_to_target(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child1 = parent.get_child_query_handler_context()
    child2 = parent.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, parent)
    with pytest.raises(RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."):
        child1.transfer_object_to(object_proxy1, child2)


def test_transfer_parent_child_check_ownership_transfer_to_target(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child1 = parent.get_child_query_handler_context()
    child2 = parent.get_child_query_handler_context()
    object_proxy1 = parent.get_temporary_table_name()
    parent.transfer_object_to(object_proxy1, child1)
    with pytest.raises(RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."):
        parent.transfer_object_to(object_proxy1, child2)


def test_transfer_siblings_checK_losing_ownership(scope_query_handler_context: ScopeQueryHandlerContext):
    child1 = scope_query_handler_context.get_child_query_handler_context()
    child2 = scope_query_handler_context.get_child_query_handler_context()
    child3 = scope_query_handler_context.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, child2)

    with pytest.raises(RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."):
        child1.transfer_object_to(object_proxy1, child3)


def test_transfer_between_siblings_object_from_different_context(
        scope_query_handler_context: ScopeQueryHandlerContext):
    child1 = scope_query_handler_context.get_child_query_handler_context()
    child2 = scope_query_handler_context.get_child_query_handler_context()
    grand_child1 = child1.get_child_query_handler_context()
    object_proxy = grand_child1.get_temporary_table_name()
    with pytest.raises(RuntimeError,
                       match="Object not owned by this ScopeQueryHandlerContext."):
        child1.transfer_object_to(object_proxy, child2)


def test_transfer_between_child_and_parent(scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    object_proxy1 = child.get_temporary_table_name()
    object_proxy2 = child.get_temporary_table_name()
    child.transfer_object_to(object_proxy1, parent)
    child.release()

    with not_raises(Exception):
        _ = object_proxy1.name
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        _ = object_proxy2.name


def test_transfer_between_parent_and_child(scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    object_proxy = parent.get_temporary_table_name()
    parent.transfer_object_to(object_proxy, child)
    child.release()

    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        value = object_proxy.name


def test_illegal_transfer_between_grand_child_and_parent(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    object_proxy = grand_child.get_temporary_table_name()
    with pytest.raises(RuntimeError, match="Given ScopeQueryHandlerContext not a child, parent or sibling."):
        grand_child.transfer_object_to(object_proxy, parent)


def test_illegal_transfer_between_parent_and_grand_child(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    object_proxy = parent.get_temporary_table_name()
    with pytest.raises(RuntimeError,
                       match="Given ScopeQueryHandlerContext not a child, parent or sibling.|"
                             "Given ScopeQueryHandlerContext not a child."):
        parent.transfer_object_to(object_proxy, grand_child)


def test_release_parent_before_child_with_temporary_object_expect_exception(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    _ = child.get_temporary_table_name()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_child_without_temporary_object_expect_exception(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    _ = scope_query_handler_context.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_grand_child_with_temporary_object_expect_exception(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    _ = grand_child.get_temporary_table_name()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_grand_child_without_temporary_object_expect_exception(
        scope_query_handler_context: ScopeQueryHandlerContext):
    parent = scope_query_handler_context
    child = scope_query_handler_context.get_child_query_handler_context()
    _ = child.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_cleanup_parent_before_grand_child_without_temporary_objects(
        scope_query_handler_context: ScopeQueryHandlerContext):
    child1 = scope_query_handler_context.get_child_query_handler_context()
    child2 = scope_query_handler_context.get_child_query_handler_context()
    _ = child1.get_child_query_handler_context()
    _ = child2.get_child_query_handler_context()
    _ = child1.get_child_query_handler_context()
    _ = child2.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError) as e:
        scope_query_handler_context.release()

    not_released_contexts = e.value.get_all_not_released_contexts()
    f = "f"
    assert len(not_released_contexts) == 6


def test_using_table_name_proxy_in_table(scope_query_handler_context: ScopeQueryHandlerContext):
    table_name = scope_query_handler_context.get_temporary_table_name()
    table = Table(table_name,
                  columns=[
                      (
                          ColumnBuilder().
                          with_name(ColumnName("COLUMN1"))
                          .with_type(ColumnType("VARCHAR"))
                          .build()
                      )
                  ])
    assert table.name is not None


def test_using_view_name_proxy_in_view(scope_query_handler_context: ScopeQueryHandlerContext):
    view_name = scope_query_handler_context.get_temporary_view_name()
    view = View(view_name, columns=[
        (
            ColumnBuilder().
            with_name(ColumnName("COLUMN1"))
            .with_type(ColumnType("VARCHAR"))
            .build()
        )])
    assert view.name is not None


def test_get_connection_existing_connection(
        scope_query_handler_context: ScopeQueryHandlerContext,
        test_connection: Connection
):
    connection = scope_query_handler_context.get_connection("existing")
    assert connection == connection


def test_get_connection_not_existing_connection(
        scope_query_handler_context: ScopeQueryHandlerContext):
    with pytest.raises(KeyError):
        scope_query_handler_context.get_connection("not_existing")
