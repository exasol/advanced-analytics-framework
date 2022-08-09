from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.top_level_event_handler_context import \
    TopLevelEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.query.drop_table_query import DropTableQuery


def test_cleanup_invalid_db_object_proxies_after_release(
        top_level_event_handler_context: TopLevelEventHandlerContext):
    proxy = top_level_event_handler_context.get_temporary_table()
    proxy_name = proxy.name()
    top_level_event_handler_context.release()
    queries = top_level_event_handler_context.cleanup_invalid_object_proxies()
    assert len(queries) == 1 and isinstance(queries[0], DropTableQuery) \
           and queries[0].get_query_str() == f"DROP TABLE {proxy_name};"


def test_cleanup_invalid_bucketfs_object_proxies_after_release(
        top_level_event_handler_context: TopLevelEventHandlerContext,
        bucketfs_location: AbstractBucketFSLocation,
        prefix: str):
    proxy = top_level_event_handler_context.get_temporary_bucketfs_file()
    bucket_file_name = "test_file.txt"
    proxy.bucketfs_location().upload_string_to_bucketfs(bucket_file_name, "test")
    top_level_event_handler_context.release()
    top_level_event_handler_context.cleanup_invalid_object_proxies()
    file_list = bucketfs_location.list_files_in_bucketfs("")
    assert file_list == []
