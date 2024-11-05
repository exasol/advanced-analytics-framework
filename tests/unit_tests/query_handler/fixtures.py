import pytest
import exasol.bucketfs as bfs

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext, Connection
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext, ConnectionLookup

PREFIX = "PREFIX"

SCHEMA = "TEMP_SCHEMA"


@pytest.fixture
def tmp_db_obj_prefix() -> str:
    return PREFIX


@pytest.fixture
def aaf_pytest_db_schema() -> str:
    return SCHEMA


class ConnectionMock(Connection):

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
def connection_mock() -> Connection:
    return ConnectionMock()


@pytest.fixture
def connection_lookup_mock(connection_mock) -> ConnectionLookup:
    def lookup(name: str) -> Connection:
        if name == connection_mock.name:
            return connection_mock
        else:
            raise KeyError()

    return lookup


@pytest.fixture
def sample_mounted_bucket(tmp_path):
    return bfs.MountedBucket(base_path=str(tmp_path))


@pytest.fixture
def sample_bucketfs_location(sample_mounted_bucket):
    return bfs.path.BucketPath("a/b", sample_mounted_bucket)


@pytest.fixture
def mocked_temporary_bucketfs_location(tmp_path):
    mounted_bucket = bfs.MountedBucket(base_path=str(tmp_path / "bucketfs"))
    return bfs.path.BucketPath("", mounted_bucket)


@pytest.fixture
def top_level_query_handler_context_mock(
        sample_bucketfs_location: bfs.path.PathLike,
        tmp_db_obj_prefix: str,
        aaf_pytest_db_schema: str,
        connection_lookup_mock: ConnectionLookup) -> TopLevelQueryHandlerContext:
    query_handler_context = TopLevelQueryHandlerContext(
        temporary_bucketfs_location=sample_bucketfs_location,
        temporary_db_object_name_prefix=tmp_db_obj_prefix,
        connection_lookup=connection_lookup_mock,
        temporary_schema_name=aaf_pytest_db_schema,
    )
    return query_handler_context


@pytest.fixture(params=["top", "child"])
def scope_query_handler_context_mock(
        top_level_query_handler_context_mock: TopLevelQueryHandlerContext,
        request) -> ScopeQueryHandlerContext:
    if request.param == "top":
        return top_level_query_handler_context_mock
    else:
        return top_level_query_handler_context_mock.get_child_query_handler_context()
