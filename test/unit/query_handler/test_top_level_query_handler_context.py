import exasol.bucketfs as bfs
import pytest

from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    ChildContextNotReleasedError,
    TopLevelQueryHandlerContext,
)
from exasol.analytics.query_handler.query.drop.table import DropTableQuery
from exasol.analytics.query_handler.query.drop.view import DropViewQuery


@pytest.fixture
def context_mock(top_level_query_handler_context_mock) -> TopLevelQueryHandlerContext:
    return top_level_query_handler_context_mock


def test_cleanup_released_temporary_table_proxies(context_mock):
    proxy = context_mock.get_temporary_table_name()
    proxy_fully_qualified = proxy.fully_qualified
    context_mock.release()
    queries = context_mock.cleanup_released_object_proxies()
    assert (
        len(queries) == 1
        and isinstance(queries[0], DropTableQuery)
        and queries[0].query_string == f"DROP TABLE IF EXISTS {proxy_fully_qualified};"
    )


def test_cleanup_released_temporary_view_proxies(context_mock):
    proxy = context_mock.get_temporary_view_name()
    proxy_fully_qualified = proxy.fully_qualified
    context_mock.release()
    queries = context_mock.cleanup_released_object_proxies()

    assert (
        len(queries) == 1
        and isinstance(queries[0], DropViewQuery)
        and queries[0].query_string == f"DROP VIEW IF EXISTS {proxy_fully_qualified};"
    )


def test_cleanup_released_bucketfs_object_with_uploaded_file_proxies(
    context_mock, sample_bucketfs_location: bfs.path.PathLike
):
    proxy = context_mock.get_temporary_bucketfs_location()
    # create dummy file with content "test"
    (proxy.bucketfs_location() / "test_file.txt").write(b"test")
    context_mock.release()
    context_mock.cleanup_released_object_proxies()
    assert not sample_bucketfs_location.is_dir()


def test_cleanup_released_bucketfs_object_without_uploaded_file_proxies_after_release(
    context_mock,
):
    _ = context_mock.get_temporary_bucketfs_location()
    context_mock.release()
    context_mock.cleanup_released_object_proxies()


def test_cleanup_release_in_reverse_order_at_top_level(context_mock):
    proxies = [context_mock.get_temporary_table_name() for _ in range(10)]
    table_names = [proxy.fully_qualified for proxy in proxies]
    context_mock.release()
    query_objects = context_mock.cleanup_released_object_proxies()
    actual_queries = [query.query_string for query in query_objects]
    expected_queries = [
        f"DROP TABLE IF EXISTS {table_name};" for table_name in reversed(table_names)
    ]
    assert expected_queries == actual_queries


def test_cleanup_release_in_reverse_order_at_child(context_mock):
    parent_proxies = [context_mock.get_temporary_table_name() for _ in range(10)]

    child = context_mock.get_child_query_handler_context()
    child_proxies = [child.get_temporary_table_name() for _ in range(10)]
    child_table_names = [proxy.fully_qualified for proxy in child_proxies]
    child.release()
    child_query_objects = context_mock.cleanup_released_object_proxies()
    child_actual_queries = [query.query_string for query in child_query_objects]
    child_expected_queries = [
        f"DROP TABLE IF EXISTS {table_name};"
        for table_name in reversed(child_table_names)
    ]

    parent_proxies.extend([context_mock.get_temporary_table_name() for _ in range(10)])
    parent_table_names = [proxy.fully_qualified for proxy in parent_proxies]
    context_mock.release()
    parent_query_objects = context_mock.cleanup_released_object_proxies()
    parent_actual_queries = [query.query_string for query in parent_query_objects]
    parent_expected_queries = [
        f"DROP TABLE IF EXISTS {table_name};"
        for table_name in reversed(parent_table_names)
    ]
    assert (
        child_expected_queries == child_actual_queries
        and parent_expected_queries == parent_actual_queries
    )


def test_cleanup_parent_before_grand_child_with_temporary_objects(context_mock):
    _ = context_mock.get_temporary_table_name()
    child1 = context_mock.get_child_query_handler_context()
    _ = child1.get_temporary_table_name()
    child2 = context_mock.get_child_query_handler_context()
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
        context_mock.release()
    cleanup_queries = context_mock.cleanup_released_object_proxies()
    assert len(cleanup_queries) == 7
