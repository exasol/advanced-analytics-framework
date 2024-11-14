from pathlib import PurePosixPath

import pytest
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import LocalFSMockBucketFSLocation

from exasol_machine_learning_library.execution.sql_stage_graph_execution.object_proxy_reference_counting_bag import \
    ObjectProxyReferenceCountingBag

pytest_plugins = [
    "tests.fixtures.top_level_query_handler_context_fixture",
]


def test_single_add(query_handler_context_with_local_bucketfs_and_no_connection):
    """
    This tests adds a object_proxy to a ObjectProxyReferenceCountingBag and checks if no objects got released
    """
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0


def test_single_add_remove(query_handler_context_with_local_bucketfs_and_no_connection):
    """
    This tests adds and removes a object_proxy to a ObjectProxyReferenceCountingBag
    and checks if one object proxy was released
    """
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    bag.remove(table_name)
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_single_add_remove_only_the_added_object_proxy_get_removed(
        query_handler_context_with_local_bucketfs_and_no_connection):
    """
    This tests adds and removes a object_proxy to a ObjectProxyReferenceCountingBag. Further, it creates an
    additional object proxy which it doesn't add and check if only one proxy was released.
    """
    query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    bag.remove(table_name)
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_single_add_different_object_proxies_remove_one(query_handler_context_with_local_bucketfs_and_no_connection):
    """
    This tests adds two object proxies to a ObjectProxyReferenceCountingBag and removes one.
    It then checks if one object proxy was released.
    """
    table_name1 = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    table_name2 = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name1)
    bag.add(table_name2)
    bag.remove(table_name1)
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_single_add_remove_add(query_handler_context_with_local_bucketfs_and_no_connection):
    """
    This tests adds and removes a object proxy to a ObjectProxyReferenceCountingBag and then attempts to add it again.
    We expect the second add to fail, because top_level_query_handler_context already disowned the object.
    """
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    bag.remove(table_name)
    with pytest.raises(Exception, match="Object not owned by this ScopeQueryHandlerContext."):
        bag.add(table_name)


def test_transfer_back_to_parent_query_handler_context_after_add(
        query_handler_context_with_local_bucketfs_and_no_connection):
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    bag.transfer_back_to_parent_query_handler_context(table_name)
    assert table_name not in bag
    query_handler_context_with_local_bucketfs_and_no_connection.release()
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_add_after_transfer_back_to_parent_query_handler_context(
        query_handler_context_with_local_bucketfs_and_no_connection):
    table_name = query_handler_context_with_local_bucketfs_and_no_connection.get_temporary_table_name()
    bag = ObjectProxyReferenceCountingBag(query_handler_context_with_local_bucketfs_and_no_connection)
    bag.add(table_name)
    bag.transfer_back_to_parent_query_handler_context(table_name)
    bag.add(table_name)
    assert table_name in bag
