from exasol_advanced_analytics_framework.deployment.scripts_deployer import \
    ScriptsDeployer
from tests.utils.db_queries import DBQueries
from tests.utils.parameters import db_params


def test_scripts_deployer(deployed_slc, language_alias, pyexasol_connection, request):
    schema_name = request.node.name
    pyexasol_connection.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")
    ScriptsDeployer.run2(
        pyexasol_connection
        # dsn=db_params.address(),
        # user=db_params.user,
        # password=db_params.password,
        schema=schema_name,
        language_alias=language_alias,
        develop=True)
    assert DBQueries.check_all_scripts_deployed(
        pyexasol_connection, schema_name)
