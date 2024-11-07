import pytest
import pyexasol
from typing import Any, Tuple, Callable
from exasol.analytics.deployment.scripts_deployer import ScriptsDeployer
from exasol.analytics.deployment.aaf_exasol_lua_script_generator import \
    save_aaf_query_loop_lua_script


BUCKETFS_CONNECTION_NAME = "TEST_AAF_BFS_CONN"


@pytest.fixture(scope="session")
def itest_db_schema() -> str:
    """
    Overrides default fixture from pytest-exasol-extension.
    """
    return "TEST_INTEGRATION"


@pytest.fixture(scope="module")
def deployed_scripts(pyexasol_connection, itest_db_schema, language_alias) -> None:
    save_aaf_query_loop_lua_script()
    ScriptsDeployer(
        language_alias,
        itest_db_schema,
        pyexasol_connection,
    ).deploy_scripts()


@pytest.fixture(scope="module")
def database_with_slc(
        deployed_scripts,
        itest_db_schema,
        bucketfs_connection_factory,
        deployed_slc,
) -> Tuple[str, str]:
    bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    return BUCKETFS_CONNECTION_NAME, itest_db_schema
