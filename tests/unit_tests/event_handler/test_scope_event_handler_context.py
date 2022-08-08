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
    actual_path = proxy.bucketfs_location().get_complete_file_path_in_bucket(None)
    expected_prefix_path = bucketfs_location.get_complete_file_path_in_bucket(None)
    assert expected_prefix_path in actual_path


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
    path1 = proxy1.bucketfs_location().get_complete_file_path_in_bucket(None)
    path2 = proxy2.bucketfs_location().get_complete_file_path_in_bucket(None)
    assert path1 != path2


def test_use_table_proxy_after_relase_fails(scope_event_handler_context: ScopeEventHandlerContext):
    proxy = scope_event_handler_context.get_temporary_table()
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="TableProxy.* already released."):
        proxy_name = proxy.name()


def test_use_view_proxy_after_relase_fails(scope_event_handler_context: ScopeEventHandlerContext):
    proxy = scope_event_handler_context.get_temporary_view()
    scope_event_handler_context.release()
    with pytest.raises(RuntimeError, match="ViewProxy.* already released."):
        proxy_name = proxy.name()
