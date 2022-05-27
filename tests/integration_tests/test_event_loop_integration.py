import json
import textwrap
from datetime import datetime, timedelta
from tests.test_package.test_event_handlers.event_handler_test import \
    FINAL_RESULT, QUERY_LIST

QUERY_FLUSH_STATS = "flush statistics"
QUERY_AUDIT_LOGS = """
SELECT START_TIME, SQL_TEXT 
FROM EXA_STATISTICS.EXA_DBA_AUDIT_SQL
WHERE 
    session_id = CURRENT_SESSION
    AND COMMAND_NAME = 'SELECT' 
    AND START_TIME > '{start_time}'
ORDER BY START_TIME DESC;
"""
N_FETCHED_ROWS = 50


def test_event_loop_integration_with_one_iteration(
        setup_database, pyexasol_connection, upload_language_container):

    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {"schema": f"{schema_name}",
         "bucketfs_connection": f"{bucketfs_connection_name}",
         "event_handler_module": "test_event_handlers.event_handler_test",
         "event_handler_class": "EventHandlerTestWithOneIteration",
         "event_handler_parameters": "{}"})

    query = f"EXECUTE SCRIPT {schema_name}.AAF_EVENT_LOOP('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    assert result[0][0] == str(FINAL_RESULT)


def test_event_loop_integration_with_two_iteration(
        setup_database, pyexasol_connection, upload_language_container):

    # get audit logs before executing event loop
    latest_stmt = datetime.utcnow() - timedelta(hours=26)  # the largest timezone difference
    logs_before_execution = pyexasol_connection.execute(
        textwrap.dedent(QUERY_AUDIT_LOGS.format(start_time=latest_stmt))
    ).fetchmany(N_FETCHED_ROWS)
    if logs_before_execution:
        latest_stmt = logs_before_execution[0][0]

    # execute event loop
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {"schema": f"{schema_name}",
         "bucketfs_connection": f"{bucketfs_connection_name}",
         "event_handler_module": "test_event_handlers.event_handler_test",
         "event_handler_class": "EventHandlerTestWithTwoIteration",
         "event_handler_parameters": "{}"})

    query = f"EXECUTE SCRIPT {schema_name}.AAF_EVENT_LOOP('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    # get audit logs after executing event loop
    pyexasol_connection.execute(QUERY_FLUSH_STATS)
    logs_after_execution = pyexasol_connection.execute(
        textwrap.dedent(QUERY_AUDIT_LOGS.format(start_time=latest_stmt))
    ).fetchmany(N_FETCHED_ROWS)
    executed_queries = list(map(lambda x: x[1], logs_after_execution))

    assert result[0][0] == str(FINAL_RESULT) \
           and set(QUERY_LIST).issubset(set(executed_queries))

