import pytest
import pyexasol
from typing import Any, Tuple
from exasol_advanced_analytics_framework.deployment.scripts_deployer import ScriptsDeployer


BUCKETFS_CONNECTION_NAME = "TEST_AAF_BFS_CONN"
SCHEMA_NAME = "TEST_INTEGRATION"
LANGUAGE_ALIAS = "PYTHON3_AAF"


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


# leave here, needed only for UDFs and similar
# backend_aware_bucketfs_params

# bucketfs_params on prem:
# https://github.com/exasol/pytest-plugins/blob/main/pytest-backend/exasol/pytest_backend/__init__.py#L282
# {
#     'backend': BACKEND_ONPREM,
#     'url': bucketfs_config.url, # includes port
#     'username': bucketfs_config.username,
#     'password': bucketfs_config.password,
#     'service_name': 'bfsdefault',
#     'bucket_name': 'default',
#     'verify': False
# }

# bucketfs_params saas:
# https://github.com/exasol/pytest-plugins/blob/main/pytest-backend/exasol/pytest_backend/__init__.py#L299
# {
#     'backend': BACKEND_SAAS,
#     'url': saas_host,
#     'account_id': saas_account_id,
#     'database_id': backend_aware_saas_database_id,
#     'pat': saas_pat
# }


import re
def _bucket_address(bucketfs_params: dict[str, Any]) -> str:
    # pytest-plugins/pytest-slc/exasol/pytest_slc/__init__.py defines
    BFS_CONTAINER_DIRECTORY = 'container'
    host_and_port = bucketfs_params["url"]
    port = ":6666" if not re.search(":[0-9]+$", host_and_port) else ""
    bucket_name = bucketfs_params["bucket_name"]
    path_in_bucket = BFS_CONTAINER_DIRECTORY
    service_name = bucketfs_params["service_name"]
    return ( f"http://{host_and_port}{port}/{bucket_name}/"
             f"{path_in_bucket};{service_name}" )


def _create_bucketfs_connection(use_onprem, db_conn, bucketfs_params: dict[str, Any]) -> str:
    name = BUCKETFS_CONNECTION_NAME
    if use_onprem:
        # In general currently I disabled SaaS backend for AAF.
        # Question: Should/Could this work for SaaS, too?
        uri = _bucket_address(bucketfs_params)
        # uri = bucketfs_params["url"]
        user = bucketfs_params["username"]
        pwd = bucketfs_params["password"]
        db_conn.execute(
            f"CREATE OR REPLACE  CONNECTION {name} TO '{uri}' " \
            f"USER '{user}' IDENTIFIED BY '{pwd}'"
        )
    return name


@pytest.fixture(scope="module")
def database_with_slc(
        use_onprem,
        pyexasol_connection,
        backend_aware_bucketfs_params,
        upload_slc,
) -> Tuple[str|None, str]:
    schema = _create_schema(pyexasol_connection)
    _deploy_scripts(pyexasol_connection)
    bfs_conn = _create_bucketfs_connection(
        use_onprem,
        pyexasol_connection,
        backend_aware_bucketfs_params,
    )
    return bfs_conn, schema
