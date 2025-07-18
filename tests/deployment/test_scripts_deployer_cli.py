from click.testing import CliRunner

from exasol.analytics.query_handler.deployment import deploy
from exasol.analytics.query_handler.deployment.slc import LANGUAGE_ALIAS
from tests.utils.db_queries import DBQueries


def test_scripts_deployer_cli(
    upload_language_container,
    backend_aware_database_params,
    pyexasol_connection,
    request,
):
    schema_name = request.node.name
    pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    dsn = backend_aware_database_params["dsn"]
    user = backend_aware_database_params["user"]
    password = backend_aware_database_params["password"]
    args_list = [
        "scripts",
        "--dsn",
        dns,
        "--user",
        user,
        "--pass",
        password,
        "--schema",
        schema_name,
        "--language-alias",
        LANGUAGE_ALIAS,
    ]
    runner = CliRunner()
    result = runner.invoke(deploy.main, args_list)
    assert result.exit_code == 0
    assert DBQueries.check_all_scripts_deployed(pyexasol_connection, schema_name)
