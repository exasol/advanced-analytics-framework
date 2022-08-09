from contextlib import contextmanager

import pytest
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext


def test_temporary_table_prefix_in_name(scope_event_handler_context: ScopeEventHandlerContext,
                                        prefix: str):
    proxy = scope_event_handler_context.get_temporary_table()
    assert proxy.name().startswith(prefix)


def test_temporary_view_prefix_in_name(scope_event_handler_context: ScopeEventHandlerContext,
                                       prefix: str):
    proxy = scope_event_handler_context.get_temporary_view()
    assert proxy.name().startswith(prefix)


def test_temporary_bucketfs_file_prefix_in_name(bucketfs_location: BucketFSLocation,
                                                scope_event_handler_context: ScopeEventHandlerContext):
    proxy = scope_event_handler_context.get_temporary_bucketfs_file()
    actual_path = proxy.bucketfs_location().get_complete_file_path_in_bucket()
    expected_prefix_path = bucketfs_location.get_complete_file_path_in_bucket()
    assert actual_path.startswith(expected_prefix_path)


def test_two_temporary_table_are_not_equal(scope_event_handler_context: ScopeEventHandlerContext):
    proxy1 = scope_event_handler_context.get_temporary_table()
    proxy2 = scope_event_handler_context.get_temporary_table()
    assert proxy1.name() != proxy2.name()


def test_two_temporary_view_are_not_equal(scope_event_handler_context: ScopeEventHandlerContext):
    proxy1 = scope_event_handler_context.get_temporary_view()
    proxy2 = scope_event_handler_context.get_temporary_view()
    assert proxy1.name() != proxy2.name()


def test_two_temporary_bucketfs_files_are_not_equal(scope_event_handler_context: ScopeEventHandlerContext):
    proxy1 = scope_event_handler_context.get_temporary_bucketfs_file()
    proxy2 = scope_event_handler_context.get_temporary_bucketfs_file()
    path1 = proxy1.bucketfs_location().get_complete_file_path_in_bucket()
    path2 = proxy2.bucketfs_location().get_complete_file_path_in_bucket()
    assert path1 != path2


def test_use_table_proxy_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    proxy = scope_event_handler_context.get_temporary_table()
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        proxy_name = proxy.name()


def test_use_view_proxy_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    proxy = scope_event_handler_context.get_temporary_view()
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewProxy.* already released."):
        proxy_name = proxy.name()


def test_get_temporary_view_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_event_handler_context.get_temporary_view()


def test_get_temporary_table_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_event_handler_context.get_temporary_table()


def test_get_temporary_bucketfs_file_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = scope_event_handler_context.get_temporary_bucketfs_file()


def test_use_child_context_after_release_fails(scope_event_handler_context: ScopeEventHandlerContext):
    child = scope_event_handler_context.get_child_event_handler_context()
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = child.get_temporary_view()


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))


def test_transfer_between_siblings(scope_event_handler_context: ScopeEventHandlerContext):
    child1 = scope_event_handler_context.get_child_event_handler_context()
    child2 = scope_event_handler_context.get_child_event_handler_context()
    object_proxy1 = child1.get_temporary_table()
    object_proxy2 = child1.get_temporary_table()
    child1.transfer_object_to(object_proxy1, child2)
    child1.release()

    with not_raises(Exception):
        object_proxy1.name()
    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        object_proxy2.name()


def test_transfer_check_ownership_transfer_to_target(scope_event_handler_context: ScopeEventHandlerContext):
    child1 = scope_event_handler_context.get_child_event_handler_context()
    child2 = scope_event_handler_context.get_child_event_handler_context()
    object_proxy1 = child1.get_temporary_table()
    object_proxy2 = child2.get_temporary_table()
    child1.transfer_object_to(object_proxy1, child2)
    child2.transfer_object_to(object_proxy1, child1)
    child2.release()

    with not_raises(Exception):
        object_proxy1.name()
    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        object_proxy2.name()


def test_transfer_checK_losing_ownership(scope_event_handler_context: ScopeEventHandlerContext):
    child1 = scope_event_handler_context.get_child_event_handler_context()
    child2 = scope_event_handler_context.get_child_event_handler_context()
    child3 = scope_event_handler_context.get_child_event_handler_context()
    object_proxy1 = child1.get_temporary_table()
    child1.transfer_object_to(object_proxy1, child2)

    with pytest.raises(RuntimeError, match="Object not owned by this ScopeEventHandlerContext."):
        child1.transfer_object_to(object_proxy1, child3)


def test_transfer_between_siblings_object_from_different_context(
        scope_event_handler_context: ScopeEventHandlerContext):
    child1 = scope_event_handler_context.get_child_event_handler_context()
    child2 = scope_event_handler_context.get_child_event_handler_context()
    grand_child1 = child1.get_child_event_handler_context()
    object_proxy = grand_child1.get_temporary_table()
    with pytest.raises(RuntimeError,
                       match="Object not owned by this ScopeEventHandlerContext."):
        child1.transfer_object_to(object_proxy, child2)


def test_transfer_between_child_and_parent(scope_event_handler_context: ScopeEventHandlerContext):
    parent = scope_event_handler_context
    child = scope_event_handler_context.get_child_event_handler_context()
    object_proxy1 = child.get_temporary_table()
    object_proxy2 = child.get_temporary_table()
    child.transfer_object_to(object_proxy1, parent)
    child.release()

    with not_raises(Exception):
        object_proxy1.name()
    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        object_proxy2.name()


def test_transfer_between_parent_and_child(scope_event_handler_context: ScopeEventHandlerContext):
    parent = scope_event_handler_context
    child = scope_event_handler_context.get_child_event_handler_context()
    object_proxy = parent.get_temporary_table()
    parent.transfer_object_to(object_proxy, child)
    child.release()

    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        object_proxy.name()


def test_illegal_transfer_between_grand_child_and_parent(
        scope_event_handler_context: ScopeEventHandlerContext):
    parent = scope_event_handler_context
    child = scope_event_handler_context.get_child_event_handler_context()
    grand_child = child.get_child_event_handler_context()
    object_proxy = grand_child.get_temporary_table()
    with pytest.raises(RuntimeError, match="Given ScopeEventHandlerContext not a child, parent or sibling."):
        grand_child.transfer_object_to(object_proxy, parent)


def test_illegal_transfer_between_parent_and_grand_child(
        scope_event_handler_context: ScopeEventHandlerContext):
    parent = scope_event_handler_context
    child = scope_event_handler_context.get_child_event_handler_context()
    grand_child = child.get_child_event_handler_context()
    object_proxy = parent.get_temporary_table()
    with pytest.raises(RuntimeError,
                       match="Given ScopeEventHandlerContext not a child, parent or sibling.|"
                             "Given ScopeEventHandlerContext not a child."):
        parent.transfer_object_to(object_proxy, grand_child)
