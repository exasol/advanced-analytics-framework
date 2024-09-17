from click.testing import CliRunner
from exasol_advanced_analytics_framework import deploy
from tests.utils.db_queries import DBQueries
from tests.utils.parameters import db_params


@pytest.mark.skip(reason="No need to test deployer provided by PEC.")
def test_scripts_deployer_cli(upload_language_container,
                              pyexasol_connection, request):
    schema_name = request.node.name
    pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")

    language_alias = "PYTHON3_AAF"
    args_list = [
        "scripts",
        "--dsn", db_params.address(),
        "--user", db_params.user,
        "--pass", db_params.password,
        "--schema", schema_name,
        "--language-alias", language_alias
    ]
    runner = CliRunner()
    result = runner.invoke(deploy.main, args_list)
    assert result.exit_code == 0
    assert DBQueries.check_all_scripts_deployed(
        pyexasol_connection, schema_name)



