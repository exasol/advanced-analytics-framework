from typing import (
    Any,
    Callable,
    Tuple,
)

import pyexasol
import pytest

from exasol.analytics.query_handler.deployment.aaf_exasol_lua_script_generator import (
    save_aaf_query_loop_lua_script,
)
from exasol.analytics.query_handler.deployment.scripts_deployer import ScriptsDeployer

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
