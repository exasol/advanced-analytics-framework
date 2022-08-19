import pytest
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation
from exasol_udf_mock_python.connection import Connection

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext

PREFIX = "PREFIX"

SCHEMA = "TEMP_SCHEMA"


@pytest.fixture
def prefix() -> str:
    return PREFIX


@pytest.fixture
def schema() -> str:
    return SCHEMA


@pytest.fixture
def bucketfs_location(tmp_path) -> AbstractBucketFSLocation:
    model_connection = Connection(address=f"file://{tmp_path}/data")
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=model_connection.address,
        user=model_connection.user,
        pwd=model_connection.password)
    return bucketfs_location


@pytest.fixture
def top_level_query_handler_context(
        bucketfs_location: BucketFSLocation, prefix: str, schema: str) -> TopLevelQueryHandlerContext:
    query_handler_context = TopLevelQueryHandlerContext(
        temporary_bucketfs_location=bucketfs_location,
        temporary_db_object_name_prefix=prefix,
        temporary_schema_name=schema
    )
    return query_handler_context


@pytest.fixture(params=["top", "child"])
def scope_query_handler_context(
        top_level_query_handler_context: TopLevelQueryHandlerContext,
        request) -> ScopeQueryHandlerContext:
    if request.param == "top":
        return top_level_query_handler_context
    else:
        return top_level_query_handler_context.get_child_query_handler_context()
