import pytest
import exasol.bucketfs as bfs

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
def sample_mounted_bucket(tmp_path):
    return bfs.MountedBucket(base_path=str(tmp_path))


@pytest.fixture
def bucketfs_location(sample_mounted_bucket):
    return bfs.path.BucketPath("a/b", sample_mounted_bucket)


@pytest.fixture
def mocked_temporary_bucketfs_location(tmp_path):
    mounted_bucket = bfs.MountedBucket(base_path=str(tmp_path / "bucketfs"))
    return bfs.path.BucketPath("", mounted_bucket)


@pytest.fixture
def top_level_query_handler_context(
        bucketfs_location: bfs.path.PathLike,
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
