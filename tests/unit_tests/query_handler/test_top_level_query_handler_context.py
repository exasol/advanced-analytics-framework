import pytest
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext, ChildContextNotReleasedError
from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropTableQuery
from exasol_advanced_analytics_framework.query_handler.query.drop_view_query import DropViewQuery


def test_cleanup_invalid_temporary_table_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext):
    proxy = top_level_query_handler_context.get_temporary_table_name()
    proxy_fully_qualified = proxy.fully_qualified
    top_level_query_handler_context.release()
    queries = top_level_query_handler_context.cleanup_released_object_proxies()
    assert len(queries) == 1 and isinstance(queries[0], DropTableQuery) \
           and queries[0].query_string == f"DROP TABLE IF EXISTS {proxy_fully_qualified};"


def test_cleanup_invalid_temporary_view_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext):
    proxy = top_level_query_handler_context.get_temporary_view_name()
    proxy_fully_qualified = proxy.fully_qualified
    top_level_query_handler_context.release()
    queries = top_level_query_handler_context.cleanup_released_object_proxies()

    assert len(queries) == 1 and isinstance(queries[0], DropViewQuery) \
           and queries[0].query_string == f"DROP VIEW IF EXISTS {proxy_fully_qualified};"


def test_cleanup_invalid_bucketfs_object_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    proxy = top_level_query_handler_context.get_temporary_bucketfs_location()
    bucket_file_name = "test_file.txt"
    proxy.bucketfs_location().upload_string_to_bucketfs(bucket_file_name, "test")
    top_level_query_handler_context.release()
    top_level_query_handler_context.cleanup_released_object_proxies()
    file_list = bucketfs_location.list_files_in_bucketfs("")
    assert file_list == []


def test_cleanup_release_in_reverse_order_at_top_level(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    proxies = [top_level_query_handler_context.get_temporary_table_name() for _ in range(10)]
    table_names = [proxy.fully_qualified for proxy in proxies]
    top_level_query_handler_context.release()
    query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    actual_queries = [query.query_string for query in query_objects]
    expected_queries = [f"DROP TABLE IF EXISTS {table_name};"
                        for table_name in reversed(table_names)]
    assert expected_queries == actual_queries


def test_cleanup_release_in_reverse_order_at_child(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    parent_proxies = [top_level_query_handler_context.get_temporary_table_name() for _ in range(10)]

    child = top_level_query_handler_context.get_child_query_handler_context()
    child_proxies = [child.get_temporary_table_name() for _ in range(10)]
    child_table_names = [proxy.fully_qualified for proxy in child_proxies]
    child.release()
    child_query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    child_actual_queries = [query.query_string for query in child_query_objects]
    child_expected_queries = [f"DROP TABLE IF EXISTS {table_name};"
                              for table_name in reversed(child_table_names)]

    parent_proxies.extend([top_level_query_handler_context.get_temporary_table_name() for _ in range(10)])
    parent_table_names = [proxy.fully_qualified for proxy in parent_proxies]
    top_level_query_handler_context.release()
    parent_query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    parent_actual_queries = [query.query_string for query in parent_query_objects]
    parent_expected_queries = [f"DROP TABLE IF EXISTS {table_name};"
                               for table_name in reversed(parent_table_names)]
    assert child_expected_queries == child_actual_queries and \
           parent_expected_queries == parent_actual_queries


def test_cleanup_parent_before_grand_child_with_temporary_objects(
        top_level_query_handler_context: TopLevelQueryHandlerContext):
    _ = top_level_query_handler_context.get_temporary_table_name()
    child1 = top_level_query_handler_context.get_child_query_handler_context()
    _ = child1.get_temporary_table_name()
    child2 = top_level_query_handler_context.get_child_query_handler_context()
    _ = child2.get_temporary_table_name()
    grand_child11 = child1.get_child_query_handler_context()
    _ = grand_child11.get_temporary_table_name()
    grand_child12 = child1.get_child_query_handler_context()
    _ = grand_child12.get_temporary_table_name()
    grand_child21 = child2.get_child_query_handler_context()
    _ = grand_child21.get_temporary_table_name()
    grand_child22 = child2.get_child_query_handler_context()
    _ = grand_child22.get_temporary_table_name()

    with pytest.raises(ChildContextNotReleasedError):
        top_level_query_handler_context.release()
    cleanup_queries = top_level_query_handler_context.cleanup_released_object_proxies()
    assert len(cleanup_queries) == 7