from click.testing import CliRunner
from exasol.analytics import deploy
from tests.utils.db_queries import DBQueries
from exasol.analytics.slc import LANGUAGE_ALIAS


def test_scripts_deployer_cli(upload_language_container,
                              backend_aware_database_params,
                              pyexasol_connection, request):
    schema_name = request.node.name
    pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    dsn = backend_aware_database_params["dsn"]
    user = backend_aware_database_params["user"]
    password = backend_aware_database_params["password"]
    args_list = [
        "scripts",
        "--dsn", dns,
        "--user", user,
        "--pass", password,
        "--schema", schema_name,
        "--language-alias", LANGUAGE_ALIAS
    ]
    runner = CliRunner()
    result = runner.invoke(deploy.main, args_list)
    assert result.exit_code == 0
    assert DBQueries.check_all_scripts_deployed(
        pyexasol_connection, schema_name)



