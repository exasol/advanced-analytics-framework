import json
import textwrap
from click.testing import CliRunner
from exasol_advanced_analytics_framework import deploy
from tests.test_package.test_event_handlers.event_handler_test import \
    FINAL_RESULT
from tests.utils.parameters import bucketfs_params, db_params


def _create_schema(db_conn, schema):
    db_conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def _deploy_scripts(language_alias, schema_name):
    args_list = [
        "scripts",
        "--dsn", db_params.address(),
        "--user", db_params.user,
        "--pass", db_params.password,
        "--schema", schema_name,
        "--language-alias", language_alias,
        "--develop"
    ]
    runner = CliRunner()
    result = runner.invoke(deploy.main, args_list)
    assert result.exit_code == 0


def _create_bucketfs_connection(db_conn, bucketfs_connection_name):
    query = "CREATE OR REPLACE  CONNECTION {name} TO '{uri}' " \
            "USER '{user}' IDENTIFIED BY '{pwd}'".format(
        name=bucketfs_connection_name,
        uri=bucketfs_params.address(),
        user=bucketfs_params.user,
        pwd=bucketfs_params.password)
    db_conn.execute(query)


def setup_database(language_alias, schema, db_conn,
                   bucketfs_connection_name):
    _create_schema(db_conn, schema)
    _deploy_scripts(language_alias, schema)
    _create_bucketfs_connection(db_conn, bucketfs_connection_name)


def test_event_loop_integration(request, pyexasol_connection,
                                upload_language_container):
    schema_name = request.node.name
    bucketfs_connection_name = "TEST_AAF_BFS_CONN"
    language_alias = "PYTHON3_AAF"
    setup_database(
        language_alias=language_alias,
        schema=schema_name,
        db_conn=pyexasol_connection,
        bucketfs_connection_name=bucketfs_connection_name)

    args = json.dumps(
        {"schema": f"{schema_name}",
         "bucketfs_connection": f"{bucketfs_connection_name}",
         "event_handler_module": "test_event_handlers.event_handler_test",
         "event_handler_class": "EventHandlerTest",
         "event_handler_parameters": "{}"})

    query = f"EXECUTE SCRIPT {schema_name}.AAF_EVENT_LOOP('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    assert result[0][0] == str(FINAL_RESULT)


