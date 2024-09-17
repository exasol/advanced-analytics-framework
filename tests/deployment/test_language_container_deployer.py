import textwrap
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_advanced_analytics_framework.deployment.language_container_deployer \
    import LanguageContainerDeployer
from tests.utils.parameters import bucketfs_params
from tests.utils.revert_language_settings import revert_language_settings_old
from tests.utils.db_queries import DBQueries
from pathlib import Path


@revert_language_settings_old
def _call_deploy_language_container_deployer(
        language_alias, schema, db_conn, container_path, language_settings):
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # call language container deployer
    bucket_fs_factory = BucketFSFactory()
    bucketfs_location = bucket_fs_factory.create_bucketfs_location(
        url=f"http://{bucketfs_params.host}:{bucketfs_params.port}/"
            f"{bucketfs_params.bucket}/{bucketfs_params.path_in_bucket};"
            f"{bucketfs_params.name}",
        user=f"{bucketfs_params.user}",
        pwd=f"{bucketfs_params.password}",
        base_path=None)
    language_container_deployer = LanguageContainerDeployer(
        db_conn, language_alias, bucketfs_location, container_path)
    language_container_deployer.deploy_container()

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


def test_language_container_deployer(
        request, pyexasol_connection, language_container):
    schema_name = request.node.name
    language_settings = DBQueries.get_language_settings(pyexasol_connection)

    result = _call_deploy_language_container_deployer(
        "PYTHON_AAF_DEPLOY_TEST",
        schema_name,
        pyexasol_connection,
        Path(language_container["container_path"]),
        language_settings
    )

    assert result[0][0]


