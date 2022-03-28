import textwrap
from exasol_advanced_analytics_framework.deployment.language_container_deployer \
    import LanguageContainerDeployer
from tests.utils.revert_language_settings import revert_language_settings
from tests.utils.db_queries import DBQueries
from tests.utils.parameters import bucketfs_params, db_params
from pathlib import Path


@revert_language_settings
def _call_deploy_language_container_deployer_cli(
        language_alias, schema, db_conn, container_path, language_settings):
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    LanguageContainerDeployer.run(
        bucketfs_name=bucketfs_params.name,
        bucketfs_host=bucketfs_params.host,
        bucketfs_port=int(bucketfs_params.port),
        bucketfs_use_https=False,
        bucketfs_user=bucketfs_params.user,
        bucketfs_password=bucketfs_params.password,
        bucket=bucketfs_params.bucket,
        path_in_bucket=bucketfs_params.path_in_bucket,
        container_file=container_path,
        dsn=db_params.address(),
        db_user=db_params.user,
        db_password=db_params.password,
        language_alias=language_alias
    )

    db_conn.execute(textwrap.dedent(f"""
    CREATE OR REPLACE {language_alias} SCALAR SCRIPT "TEST_UDF"()
    RETURNS BOOLEAN AS

    def run(ctx):
        return True

    /
    """))
    result = db_conn.execute('SELECT "TEST_UDF"()').fetchall()
    return result


def test_language_container_deployer(
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


