import json
import textwrap
from typing import Tuple, List

import pyexasol
import pytest

from tests.test_package.test_query_handlers.query_handler_test import \
    FINAL_RESULT, QUERY_LIST
from tests.utils.parameters import db_params

QUERY_FLUSH_STATS = """FLUSH STATISTICS"""
QUERY_AUDIT_LOGS = """
SELECT SQL_TEXT 
FROM EXA_STATISTICS.EXA_DBA_AUDIT_SQL
WHERE SESSION_ID = CURRENT_SESSION
ORDER BY START_TIME DESC;
"""
N_FETCHED_ROWS = 50


def test_query_loop_integration_with_one_iteration(
        setup_database, pyexasol_connection, upload_language_container):
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "query_handler": {
                "class": {
                    "name": "QueryHandlerTestWithOneIteration",
                    "module": "test_query_handlers.query_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_QUERY_HANDLER_UDF"
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

    query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    assert result[0][0] == str(FINAL_RESULT)


def test_query_loop_integration_with_one_iteration_with_not_released_child_query_handler_context(
        setup_database, upload_language_container):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries into its own session for easer retrieval
    conn = pyexasol.connect(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password)

    # execute query loop
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "query_handler": {
                "class": {
                    "name": "QueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext",
                    "module": "test_query_handlers.query_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_QUERY_HANDLER_UDF"
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
    with pytest.raises(pyexasol.ExaQueryError) as caught_exception:
        query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
        result = conn.execute(textwrap.dedent(query)).fetchall()
    assert "E-AAF-4: Error occurred while calling the query handler." in caught_exception.value.message and \
           "RuntimeError: Child contexts are not released" in caught_exception.value.message


def test_query_loop_integration_with_one_iteration_with_not_released_temporary_object(
        setup_database, upload_language_container):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries into its own session for easer retrieval
    conn = pyexasol.connect(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password)

    # execute query loop
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "query_handler": {
                "class": {
                    "name": "QueryHandlerWithOneIterationWithNotReleasedTemporaryObject",
                    "module": "test_query_handlers.query_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_QUERY_HANDLER_UDF"
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
    with pytest.raises(pyexasol.ExaQueryError) as caught_exception:
        query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
        result = conn.execute(textwrap.dedent(query)).fetchall()
    assert "E-AAF-4: Error occurred while calling the query handler." in caught_exception.value.message and \
           "RuntimeError: Child contexts are not released" in caught_exception.value.message

    # get audit logs after executing query loop
    conn.execute(QUERY_FLUSH_STATS)
    audit_logs: List[Tuple[str]] = conn.execute(textwrap.dedent(QUERY_AUDIT_LOGS)) \
        .fetchmany(N_FETCHED_ROWS)
    executed_queries = [row[0] for row in audit_logs]
    table_cleanup_query = [query for query in executed_queries if
                           query.startswith(f'DROP TABLE IF EXISTS "{schema_name}"."DB1_')]
    assert table_cleanup_query


def test_query_loop_integration_with_two_iteration(
        setup_database, upload_language_container):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries into its own session for easer retrieval
    conn = pyexasol.connect(
        dsn=db_params.address(),
        user=db_params.user,
        password=db_params.password)

    # execute query loop
    bucketfs_connection_name, schema_name = setup_database
    args = json.dumps(
        {
            "query_handler": {
                "class": {
                    "name": "QueryHandlerTestWithTwoIteration",
                    "module": "test_query_handlers.query_handler_test"
                },
                "udf": {
                    "schema": schema_name,
                    "name": "AAF_QUERY_HANDLER_UDF"
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
    query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
    result = conn.execute(textwrap.dedent(query)).fetchall()

    # get audit logs after executing query loop
    conn.execute(QUERY_FLUSH_STATS)
    audit_logs: List[Tuple[str]] = conn.execute(textwrap.dedent(QUERY_AUDIT_LOGS)) \
        .fetchmany(N_FETCHED_ROWS)
    executed_queries = [row[0] for row in audit_logs]
    view_cleanup_query = [query for query in executed_queries if
                          query.startswith(f'DROP VIEW IF EXISTS "{schema_name}"."DB1_')]
    expected_query_list = {query.query_string for query in QUERY_LIST}
    select_queries_from_query_handler = {query for query in executed_queries
                                         if query in expected_query_list}
    # TODO build an assert which can find a list of regex as a subsequence of a list of strings,
    #  see https://kalnytskyi.com/posts/assert-str-matches-regex-in-pytest/
    # asserts
    assert result[0][0] == str(FINAL_RESULT) \
           and select_queries_from_query_handler == expected_query_list \
           and view_cleanup_query, \
        f"Not all required queries where executed {executed_queries}"
