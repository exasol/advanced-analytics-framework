import pytest
from typing import Tuple
from exasol_advanced_analytics_framework.deployment.scripts_deployer import \
    ScriptsDeployer
from tests.utils.parameters import db_params, bucketfs_params


bucketfs_connection_name = "TEST_AAF_BFS_CONN"
schema_name = "TEST_INTEGRATION"
language_alias = "PYTHON3_AAF"


def _create_schema(db_conn) -> None:
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")


def _deploy_scripts() -> None:
    ScriptsDeployer.run(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password,
        schema=schema_name,
        language_alias=language_alias,
        develop=True)


def _create_bucketfs_connection(db_conn) -> None:
    query = "CREATE OR REPLACE  CONNECTION {name} TO '{uri}' " \
            "USER '{user}' IDENTIFIED BY '{pwd}'".format(
        name=bucketfs_connection_name,
        uri=bucketfs_params.address(bucketfs_params.real_port),
        user=bucketfs_params.user,
        pwd=bucketfs_params.password)
    db_conn.execute(query)


@pytest.fixture(scope="module")
def setup_database(pyexasol_connection) -> Tuple[str, str]:
    _create_schema(pyexasol_connection)
    _deploy_scripts()
    _create_bucketfs_connection(pyexasol_connection)
    return bucketfs_connection_name, schema_name
