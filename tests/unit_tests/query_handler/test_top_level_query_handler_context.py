from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropTableQuery
from exasol_advanced_analytics_framework.query_handler.query.drop_view_query import DropViewQuery


def test_cleanup_invalid_temporary_table_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext):
    proxy = top_level_query_handler_context.get_temporary_table()
    proxy_name = proxy.name()
    top_level_query_handler_context.release()
    queries = top_level_query_handler_context.cleanup_released_object_proxies()
    assert len(queries) == 1 and isinstance(queries[0], DropTableQuery) \
           and queries[0].query_string == f"DROP TABLE IF EXISTS {proxy_name.fully_qualified()};"


def test_cleanup_invalid_temporary_view_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext):
    proxy = top_level_query_handler_context.get_temporary_view()
    proxy_name = proxy.name()
    top_level_query_handler_context.release()
    queries = top_level_query_handler_context.cleanup_released_object_proxies()
    assert len(queries) == 1 and isinstance(queries[0], DropViewQuery) \
           and queries[0].query_string == f"DROP VIEW IF EXISTS {proxy_name.fully_qualified()};"


def test_cleanup_invalid_bucketfs_object_proxies_after_release(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    proxy = top_level_query_handler_context.get_temporary_bucketfs_file()
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
    proxies = [top_level_query_handler_context.get_temporary_table() for _ in range(10)]
    table_names = [proxy.name() for proxy in proxies]
    top_level_query_handler_context.release()
    query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    actual_queries = [query.query_string for query in query_objects]
    expected_queries = [f"DROP TABLE IF EXISTS {table_name.fully_qualified()};"
                        for table_name in reversed(table_names)]
    assert expected_queries == actual_queries


def test_cleanup_release_in_reverse_order_at_child(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    parent_proxies = [top_level_query_handler_context.get_temporary_table() for _ in range(10)]

    child = top_level_query_handler_context.get_child_query_handler_context()
    child_proxies = [child.get_temporary_table() for _ in range(10)]
    child_table_names = [proxy.name() for proxy in child_proxies]
    child.release()
    child_query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    child_actual_queries = [query.query_string for query in child_query_objects]
    child_expected_queries = [f"DROP TABLE IF EXISTS {table_name.fully_qualified()};"
                              for table_name in reversed(child_table_names)]

    parent_proxies.extend([top_level_query_handler_context.get_temporary_table() for _ in range(10)])
    parent_table_names = [proxy.name() for proxy in parent_proxies]
    top_level_query_handler_context.release()
    parent_query_objects = top_level_query_handler_context.cleanup_released_object_proxies()
    parent_actual_queries = [query.query_string for query in parent_query_objects]
    parent_expected_queries = [f"DROP TABLE IF EXISTS {table_name.fully_qualified()};"
                               for table_name in reversed(parent_table_names)]
    assert child_expected_queries == child_actual_queries and \
           parent_expected_queries == parent_actual_queries
