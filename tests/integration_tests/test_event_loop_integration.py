import json
import textwrap
import pyexasol
from tests.test_package.test_event_handlers.event_handler_test import \
    FINAL_RESULT, QUERY_LIST
from tests.utils.parameters import db_params

QUERY_FLUSH_STATS = """FLUSH STATISTICS"""
QUERY_AUDIT_LOGS = """
SELECT SQL_TEXT 
FROM EXA_STATISTICS.EXA_DBA_AUDIT_SQL
WHERE COMMAND_NAME = 'SELECT' AND SESSION_ID = CURRENT_SESSION
ORDER BY START_TIME DESC;
"""
N_FETCHED_ROWS = 50


def test_event_loop_integration_with_one_iteration(
        setup_database, pyexasol_connection, upload_language_container):
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "event_handler": {
                "class": {
                    "name": "EventHandlerTestWithOneIteration",
                    "module": "test_event_handlers.event_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_EVENT_HANDLER_UDF"
                },
                "parameters": "{}"
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name
                },
                "schema_name": schema_name
            }
        })

    query = f"EXECUTE SCRIPT {schema_name}.AAF_EVENT_LOOP('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    assert result[0][0] == str(FINAL_RESULT)


def test_event_loop_integration_with_two_iteration(
        setup_database, upload_language_container):
    # start a new db session, to isolate the EXECUTE SCRIPT and the EventHandler queries into its own session for easer retrieval
    conn = pyexasol.connect(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password)

    # execute event loop
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "event_handler": {
                "class": {
                    "name": "EventHandlerTestWithTwoIteration",
                    "module": "test_event_handlers.event_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_EVENT_HANDLER_UDF"
                },
                "parameters": "{}"
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name
                },
                "schema_name": schema_name
            }
        })
    query = f"EXECUTE SCRIPT {schema_name}.AAF_EVENT_LOOP('{args}')"
    result = conn.execute(textwrap.dedent(query)).fetchall()

    # get audit logs after executing event loop
    conn.execute(QUERY_FLUSH_STATS)
    audit_logs = conn.execute(textwrap.dedent(QUERY_AUDIT_LOGS)) \
        .fetchmany(N_FETCHED_ROWS)
    executed_queries = list(map(lambda x: x[0], audit_logs))

    # asserts
    assert result[0][0] == str(FINAL_RESULT) \
           and set(QUERY_LIST).issubset(set(executed_queries))
