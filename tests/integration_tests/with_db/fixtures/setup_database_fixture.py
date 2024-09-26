import pytest
import pyexasol
from typing import Any, Tuple, Callable
from exasol_advanced_analytics_framework.deployment.scripts_deployer import ScriptsDeployer
from exasol_advanced_analytics_framework.deployment.aaf_exasol_lua_script_generator import \
    save_aaf_query_loop_lua_script


BUCKETFS_CONNECTION_NAME = "TEST_AAF_BFS_CONN"


@pytest.fixture(scope="session")
def db_schema_name() -> str:
    """
    Overrides default fixture from pytest-exasol-extension.
    """
    return "TEST_INTEGRATION"


@pytest.fixture(scope="module")
def deployed_scripts(pyexasol_connection, db_schema_name, language_alias) -> None:
    save_aaf_query_loop_lua_script()
    ScriptsDeployer(
        language_alias,
        db_schema_name,
        pyexasol_connection,
    ).deploy_scripts()
    # ScriptsDeployer.run(
    #     pyexasol_connection,
    #     schema=db_schema_name,
    #     language_alias=language_alias,
    #     develop=True)


# Can be removed after
# https://github.com/exasol/advanced-analytics-framework/issues/176
def _bucket_address(
        bucketfs_params: dict[str, Any],
        path_in_bucket: str = "my-folder",
) -> str:
    url = bucketfs_params["url"]
    bucket_name = bucketfs_params["bucket_name"]
    service_name = bucketfs_params["service_name"]
    return ( f"{url}/{bucket_name}/"
             f"{path_in_bucket};{service_name}" )


# Can be removed after
# https://github.com/exasol/advanced-analytics-framework/issues/176
@pytest.fixture(scope='session')
def my_bucketfs_connection_factory(
        use_onprem,
        pyexasol_connection,
        backend_aware_bucketfs_params,
) -> Callable[[str, str|None], None]:
    def create(name, path_in_bucket):
        if not use_onprem:
            return
        bucketfs_params = backend_aware_bucketfs_params
        uri = _bucket_address(bucketfs_params, path_in_bucket)
        user = bucketfs_params["username"]
        pwd = bucketfs_params["password"]
        pyexasol_connection.execute(
            f"CREATE OR REPLACE  CONNECTION {name} TO '{uri}' " \
            f"USER '{user}' IDENTIFIED BY '{pwd}'"
        )
    return create


@pytest.fixture(scope="module")
def database_with_slc(
        pyexasol_connection,
        deployed_scripts,
        db_schema_name,
        bucketfs_connection_factory,
        my_bucketfs_connection_factory,
        deployed_slc,
) -> Tuple[str|None, str]:
    # this requires updating query_handler_runner_udf.py to the new bucketfs API, first,
    # which is planned to be done in ticket
    # https://github.com/exasol/advanced-analytics-framework/issues/176
    # bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    my_bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    return BUCKETFS_CONNECTION_NAME, db_schema_name
