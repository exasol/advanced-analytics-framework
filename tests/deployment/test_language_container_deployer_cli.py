import textwrap
from click.testing import CliRunner
from exasol_advanced_analytics_framework import deploy
from tests.utils.revert_language_settings import revert_language_settings
from tests.utils.db_queries import DBQueries
from tests.utils.parameters import bucketfs_params, db_params
from pathlib import Path


@revert_language_settings
def _call_deploy_language_container_deployer_cli(
        language_alias, schema, db_conn, container_path, language_settings):
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # call language container deployer
    args_list = [
        "language-container",
        "--bucketfs-name", bucketfs_params.name,
        "--bucketfs-host", bucketfs_params.host,
        "--bucketfs-port", bucketfs_params.port,
        "--bucketfs_use-https", False,
        "--bucketfs-user", bucketfs_params.user,
        "--bucketfs-password", bucketfs_params.password,
        "--bucket", bucketfs_params.bucket,
        "--path-in-bucket", bucketfs_params.path_in_bucket,
        "--container-file", container_path,
        "--dsn", db_params.address(),
        "--db-user", db_params.user,
        "--db-pass", db_params.password,
        "--language-alias", language_alias
    ]
    runner = CliRunner()
    result = runner.invoke(deploy.main, args_list)
    assert result.exit_code == 0

    # make this db session same with the db system
    system_language_settings = DBQueries.get_language_settings_from(
        db_conn, "SYSTEM")
    DBQueries.set_language_settings_to(
        db_conn, "SESSION", system_language_settings)

    # create a sample UDF using the new language alias
    db_conn.execute(textwrap.dedent(f"""
    CREATE OR REPLACE {language_alias} SCALAR SCRIPT "TEST_UDF"()
    RETURNS BOOLEAN AS

    def run(ctx):
        return True

    /
    """))
    result = db_conn.execute('SELECT "TEST_UDF"()').fetchall()
    return result


def test_language_container_deployer_cli(
        request, pyexasol_connection, language_container):
    schema_name = request.node.name
    language_settings = DBQueries.get_language_settings(pyexasol_connection)

    result = _call_deploy_language_container_deployer_cli(
        "PYTHON_AAF_DEPLOY_TEST",
        schema_name,
        pyexasol_connection,
        Path(language_container["container_path"]),
        language_settings
    )

    assert result[0][0]


