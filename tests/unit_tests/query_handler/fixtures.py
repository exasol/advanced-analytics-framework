import pytest
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext, Connection
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext, ConnectionLookup

PREFIX = "PREFIX"

SCHEMA = "TEMP_SCHEMA"


@pytest.fixture
def prefix() -> str:
    return PREFIX


@pytest.fixture
def schema() -> str:
    return SCHEMA


class TestConnection(Connection):

    @property
    def name(self) -> str:
        return "existing"

    @property
    def address(self) -> str:
        return "address"

    @property
    def user(self) -> str:
        return "user"

    @property
    def password(self) -> str:
        return "password"


@pytest.fixture
def test_connection() -> Connection:
    return TestConnection()


@pytest.fixture
def test_connection_lookup(test_connection) -> ConnectionLookup:
    def lookup(name: str) -> Connection:
        if name == test_connection.name:
            return test_connection
        else:
            raise KeyError()

    return lookup


@pytest.fixture
def bucketfs_location(tmp_path) -> AbstractBucketFSLocation:
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=f"file://{tmp_path}/data",
        user=None,
        pwd=None)
    return bucketfs_location


@pytest.fixture
def top_level_query_handler_context(
        bucketfs_location: BucketFSLocation,
        prefix: str,
        schema: str,
        test_connection_lookup: ConnectionLookup) -> TopLevelQueryHandlerContext:
    query_handler_context = TopLevelQueryHandlerContext(
        temporary_bucketfs_location=bucketfs_location,
        temporary_db_object_name_prefix=prefix,
        connection_lookup=test_connection_lookup,
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
