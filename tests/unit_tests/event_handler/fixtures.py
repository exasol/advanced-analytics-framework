import pytest
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_udf_mock_python.connection import Connection

from exasol_advanced_analytics_framework.event_handler.context.scope_event_handler_context import \
    ScopeEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.context.top_level_event_handler_context import \
    TopLevelEventHandlerContext

PREFIX = "PREFIX"


@pytest.fixture
def prefix() -> str:
    return PREFIX


@pytest.fixture
def bucketfs_location(tmp_path) -> BucketFSLocation:
    model_connection = Connection(address=f"file://{tmp_path}/data")
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=model_connection.address,
        user=model_connection.user,
        pwd=model_connection.password)
    return bucketfs_location


@pytest.fixture
def top_level_event_handler_context(
        bucketfs_location: BucketFSLocation, prefix: str, ) -> TopLevelEventHandlerContext:
    event_handler_context = TopLevelEventHandlerContext(
        temporary_bucketfs_location=bucketfs_location,
        temporary_name_prefix=prefix)
    return event_handler_context


@pytest.fixture(params=["top", "child"])
def scope_event_handler_context(
        top_level_event_handler_context: TopLevelEventHandlerContext,
        request) -> ScopeEventHandlerContext:
    if request.param == "top":
        return top_level_event_handler_context
    else:
        return top_level_event_handler_context.get_child_event_handler_context()
