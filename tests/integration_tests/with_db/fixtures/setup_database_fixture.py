import pytest
import pyexasol
from typing import Any, Tuple, Callable
from exasol_advanced_analytics_framework.deployment.scripts_deployer import ScriptsDeployer
from exasol_advanced_analytics_framework.slc import LANGUAGE_ALIAS


BUCKETFS_CONNECTION_NAME = "TEST_AAF_BFS_CONN"
SCHEMA_NAME = "TEST_INTEGRATION"


@pytest.fixture(scope="session")
def pyexasol_connection(backend_aware_database_params) -> pyexasol.ExaConnection:
    return pyexasol.connect(**backend_aware_database_params)


def _create_schema(db_conn) -> str:
    schema = SCHEMA_NAME
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    return schema

def _deploy_scripts(db_conn) -> None:
    ScriptsDeployer.run2(
        db_conn,
        schema=SCHEMA_NAME,
        language_alias=LANGUAGE_ALIAS,
        develop=True)


def _bucket_address(
        bucketfs_params: dict[str, Any],
        path_in_bucket: str = "my-folder",
) -> str:
    url = bucketfs_params["url"]
    bucket_name = bucketfs_params["bucket_name"]
    service_name = bucketfs_params["service_name"]
    return ( f"{url}/{bucket_name}/"
             f"{path_in_bucket};{service_name}" )


def _create_bucketfs_connection(
        use_onprem,
        db_conn,
        bucketfs_params: dict[str, Any],
        name: str = BUCKETFS_CONNECTION_NAME,
        path_in_bucket: str = "my-folder",
) -> str:
    if use_onprem:
        uri = _bucket_address(bucketfs_params, path_in_bucket)
        user = bucketfs_params["username"]
        pwd = bucketfs_params["password"]
        db_conn.execute(
            f"CREATE OR REPLACE  CONNECTION {name} TO '{uri}' " \
            f"USER '{user}' IDENTIFIED BY '{pwd}'"
        )
    return name


@pytest.fixture(scope='session')
def my_bucketfs_connection_factory(
        use_onprem,
        pyexasol_connection,
        backend_aware_bucketfs_params,
) -> Callable[[str, str|None], None]:
    def create(name, path_in_bucket):
        _create_bucketfs_connection(
            use_onprem,
            pyexasol_connection,
            backend_aware_bucketfs_params,
            name,
            path_in_bucket,
        )
    return create


@pytest.fixture(scope="module")
def database_with_slc(
        use_onprem,
        pyexasol_connection,
        bucketfs_connection_factory,
        my_bucketfs_connection_factory,
        deployed_slc,
) -> Tuple[str|None, str]:
    schema = _create_schema(pyexasol_connection)
    _deploy_scripts(pyexasol_connection)
    # this requires updating query_handler_runner_udf.py to the new bucketfs API, first:
    # bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    my_bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    return BUCKETFS_CONNECTION_NAME, schema
