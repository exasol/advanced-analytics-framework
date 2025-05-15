from contextlib import contextmanager

import exasol.bucketfs as bfs
import pytest

from exasol.analytics.query_handler.context.connection_name import ConnectionName
from exasol.analytics.query_handler.context.scope import (
    Connection,
    ScopeQueryHandlerContext,
)
from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    ChildContextNotReleasedError,
)
from exasol.analytics.schema import (
    SchemaName,
    Table,
    UDFName,
    varchar_column,
    View,
)


@pytest.fixture
def context_mock(scope_query_handler_context_mock) -> ScopeQueryHandlerContext:
    return scope_query_handler_context_mock


@pytest.fixture
def prefix(tmp_db_obj_prefix: str) -> str:
    return tmp_db_obj_prefix


def test_temporary_table_prefix_in_name(context_mock, prefix):
    proxy = context_mock.get_temporary_table_name()
    assert proxy.name.startswith(prefix)


def test_temporary_table_temporary_schema(context_mock, aaf_pytest_db_schema: str):
    proxy = context_mock.get_temporary_table_name()
    assert proxy.schema_name.name == aaf_pytest_db_schema


def test_temporary_view_prefix_in_name(context_mock, prefix):
    proxy = context_mock.get_temporary_view_name()
    assert proxy.name.startswith(prefix)


def test_temporary_view_temporary_schema(context_mock, aaf_pytest_db_schema: str):
    proxy = context_mock.get_temporary_view_name()
    assert proxy.schema_name.name == aaf_pytest_db_schema


def test_temporary_connection_temporary(context_mock: ScopeQueryHandlerContext):
    proxy = context_mock.get_temporary_connection_name()
    assert isinstance(proxy, ConnectionName)


def test_temporary_udf_temporary(
    context_mock: ScopeQueryHandlerContext, aaf_pytest_db_schema: str
):
    proxy = context_mock.get_temporary_udf_name()
    assert isinstance(proxy, UDFName) and proxy.schema_name == SchemaName(
        aaf_pytest_db_schema
    )


def test_temporary_bucketfs_file_prefix_in_name(
    sample_bucketfs_location: bfs.path.PathLike, context_mock: ScopeQueryHandlerContext
):
    proxy = context_mock.get_temporary_bucketfs_location()
    actual_path = proxy.bucketfs_location().as_udf_path()
    expected_prefix_path = sample_bucketfs_location.as_udf_path()
    assert actual_path.startswith(expected_prefix_path)


def test_two_temporary_table_are_not_equal(context_mock: ScopeQueryHandlerContext):
    proxy1 = context_mock.get_temporary_table_name()
    proxy2 = context_mock.get_temporary_table_name()
    assert proxy1.name != proxy2.name


def test_two_temporary_view_are_not_equal(context_mock: ScopeQueryHandlerContext):
    proxy1 = context_mock.get_temporary_view_name()
    proxy2 = context_mock.get_temporary_view_name()
    assert proxy1.name != proxy2.name


def test_two_temporary_bucketfs_files_are_not_equal(
    context_mock: ScopeQueryHandlerContext,
):
    proxy1 = context_mock.get_temporary_bucketfs_location()
    proxy2 = context_mock.get_temporary_bucketfs_location()
    path1 = proxy1.bucketfs_location().as_udf_path()
    path2 = proxy2.bucketfs_location().as_udf_path()
    assert path1 != path2


def test_temporary_table_name_proxy_use_name_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_table_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.name


def test_temporary_view_name_proxy_use_name_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_view_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.name


def test_temporary_table_name_proxy_use_schema_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_table_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.schema_name


def test_temporary_view_name_proxy_use_schema_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_view_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.schema_name


def test_temporary_table_name_proxy_use_quoted_name_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_table_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.quoted_name


def test_temporary_view_name_proxy_use_quoted_name_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_view_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.quoted_name


def test_temporary_table_name_proxy_use_fully_qualified_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_table_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        proxy_name = proxy.fully_qualified


def test_temporary_view_name_proxy_use_fully_qualified_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    proxy = context_mock.get_temporary_view_name()
    context_mock.release()
    with pytest.raises(RuntimeError, match="ViewNameProxy.* already released."):
        proxy_name = proxy.fully_qualified


def test_get_temporary_view_after_release_fails(context_mock: ScopeQueryHandlerContext):
    context_mock.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = context_mock.get_temporary_view_name()


def test_get_temporary_table_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    context_mock.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = context_mock.get_temporary_table_name()


def test_get_temporary_bucketfs_file_after_release_fails(
    context_mock: ScopeQueryHandlerContext,
):
    context_mock.release()
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = context_mock.get_temporary_bucketfs_location()


def test_use_child_context_after_release_fails(context_mock: ScopeQueryHandlerContext):
    child = context_mock.get_child_query_handler_context()
    try:
        context_mock.release()
    except:
        pass
    with pytest.raises(RuntimeError, match="Context already released."):
        proxy = child.get_temporary_view_name()


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail(f"DID RAISE {exception}")


def test_transfer_between_siblings(context_mock: ScopeQueryHandlerContext):
    child1 = context_mock.get_child_query_handler_context()
    child2 = context_mock.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    object_proxy2 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, child2)
    child1.release()

    with not_raises(Exception):
        _2 = object_proxy1.name
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        _ = object_proxy2.name


def test_transfer_siblings_check_ownership_transfer_to_target(
    context_mock: ScopeQueryHandlerContext,
):
    child1 = context_mock.get_child_query_handler_context()
    child2 = context_mock.get_child_query_handler_context()
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
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child1 = parent.get_child_query_handler_context()
    child2 = parent.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, parent)
    with pytest.raises(
        RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."
    ):
        child1.transfer_object_to(object_proxy1, child2)


def test_transfer_parent_child_check_ownership_transfer_to_target(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child1 = parent.get_child_query_handler_context()
    child2 = parent.get_child_query_handler_context()
    object_proxy1 = parent.get_temporary_table_name()
    parent.transfer_object_to(object_proxy1, child1)
    with pytest.raises(
        RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."
    ):
        parent.transfer_object_to(object_proxy1, child2)


def test_transfer_siblings_checK_losing_ownership(
    context_mock: ScopeQueryHandlerContext,
):
    child1 = context_mock.get_child_query_handler_context()
    child2 = context_mock.get_child_query_handler_context()
    child3 = context_mock.get_child_query_handler_context()
    object_proxy1 = child1.get_temporary_table_name()
    child1.transfer_object_to(object_proxy1, child2)

    with pytest.raises(
        RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."
    ):
        child1.transfer_object_to(object_proxy1, child3)


def test_transfer_between_siblings_object_from_different_context(
    context_mock: ScopeQueryHandlerContext,
):
    child1 = context_mock.get_child_query_handler_context()
    child2 = context_mock.get_child_query_handler_context()
    grand_child1 = child1.get_child_query_handler_context()
    object_proxy = grand_child1.get_temporary_table_name()
    with pytest.raises(
        RuntimeError, match="Object not owned by this ScopeQueryHandlerContext."
    ):
        child1.transfer_object_to(object_proxy, child2)


def test_transfer_between_child_and_parent(context_mock: ScopeQueryHandlerContext):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    object_proxy1 = child.get_temporary_table_name()
    object_proxy2 = child.get_temporary_table_name()
    child.transfer_object_to(object_proxy1, parent)
    child.release()

    with not_raises(Exception):
        _ = object_proxy1.name
    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        _ = object_proxy2.name


def test_transfer_between_parent_and_child(context_mock: ScopeQueryHandlerContext):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    object_proxy = parent.get_temporary_table_name()
    parent.transfer_object_to(object_proxy, child)
    child.release()

    with pytest.raises(RuntimeError, match="TableNameProxy.* already released."):
        value = object_proxy.name


def test_illegal_transfer_between_grand_child_and_parent(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    object_proxy = grand_child.get_temporary_table_name()
    with pytest.raises(
        RuntimeError,
        match="Given ScopeQueryHandlerContext not a child, parent or sibling.",
    ):
        grand_child.transfer_object_to(object_proxy, parent)


def test_illegal_transfer_between_parent_and_grand_child(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    object_proxy = parent.get_temporary_table_name()
    with pytest.raises(
        RuntimeError,
        match="Given ScopeQueryHandlerContext not a child, parent or sibling.|"
        "Given ScopeQueryHandlerContext not a child.",
    ):
        parent.transfer_object_to(object_proxy, grand_child)


def test_release_parent_before_child_with_temporary_object_expect_exception(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    _ = child.get_temporary_table_name()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_child_without_temporary_object_expect_exception(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    _ = context_mock.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_grand_child_with_temporary_object_expect_exception(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    grand_child = child.get_child_query_handler_context()
    _ = grand_child.get_temporary_table_name()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_release_parent_before_grand_child_without_temporary_object_expect_exception(
    context_mock: ScopeQueryHandlerContext,
):
    parent = context_mock
    child = context_mock.get_child_query_handler_context()
    _ = child.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError):
        parent.release()


def test_cleanup_parent_before_grand_child_without_temporary_objects(
    context_mock: ScopeQueryHandlerContext,
):
    child1 = context_mock.get_child_query_handler_context()
    child2 = context_mock.get_child_query_handler_context()
    _ = child1.get_child_query_handler_context()
    _ = child2.get_child_query_handler_context()
    _ = child1.get_child_query_handler_context()
    _ = child2.get_child_query_handler_context()
    with pytest.raises(ChildContextNotReleasedError) as e:
        context_mock.release()

    not_released_contexts = e.value.get_all_not_released_contexts()
    f = "f"
    assert len(not_released_contexts) == 6


def test_using_table_name_proxy_in_table(context_mock: ScopeQueryHandlerContext):
    table_name = context_mock.get_temporary_table_name()
    table = Table(table_name, columns=[varchar_column("COLUMN1", size=1)])
    assert table.name is not None


def test_using_view_name_proxy_in_view(context_mock: ScopeQueryHandlerContext):
    view_name = context_mock.get_temporary_view_name()
    view = View(view_name, columns=[varchar_column("COLUMN1", size=1)])
    assert view.name is not None


def test_get_connection_existing_connection(
    context_mock: ScopeQueryHandlerContext, connection_mock: Connection
):
    connection = context_mock.get_connection("existing")
    assert connection == connection


def test_get_connection_not_existing_connection(context_mock: ScopeQueryHandlerContext):
    with pytest.raises(KeyError):
        context_mock.get_connection("not_existing")
