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


@pytest.fixture(scope="module")
def database_with_slc(
        deployed_scripts,
        db_schema_name,
        bucketfs_connection_factory,
        deployed_slc,
) -> Tuple[str, str]:
    bucketfs_connection_factory(BUCKETFS_CONNECTION_NAME, "my-folder")
    return BUCKETFS_CONNECTION_NAME, db_schema_name
