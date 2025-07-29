import json
import textwrap
from typing import (
    List,
    Tuple,
)

import pyexasol
import pytest

from tests.test_package.test_query_handlers.query_handler_test import (
    FINAL_RESULT,
    QUERY_LIST,
    TEST_INPUT,
)

QUERY_FLUSH_STATS = """FLUSH STATISTICS"""
QUERY_AUDIT_LOGS = """
SELECT SQL_TEXT
FROM EXA_STATISTICS.EXA_DBA_AUDIT_SQL
WHERE SESSION_ID = CURRENT_SESSION
ORDER BY START_TIME DESC;
"""
N_FETCHED_ROWS = 50


def test_query_loop_integration_with_one_iteration(
    database_with_slc, pyexasol_connection
):
    bucketfs_connection_name, schema_name = database_with_slc
    args = json.dumps(
        {
            "query_handler": {
                "factory_class": {
                    "name": "QueryHandlerTestWithOneIterationFactory",
                    "module": "test_query_handlers.query_handler_test",
                },
                "udf": {"schema": schema_name, "name": "AAF_QUERY_HANDLER_UDF"},
                "parameter": TEST_INPUT,
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name,
                },
                "schema_name": schema_name,
            },
        }
    )

    query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
    result = pyexasol_connection.execute(textwrap.dedent(query)).fetchall()

    assert result[0][0] == FINAL_RESULT


def test_query_loop_integration_with_one_iteration_with_not_released_child_query_handler_context(
    database_with_slc, backend_aware_database_params
):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries
    # into its own session, for easier retrieval
    conn = pyexasol.connect(**backend_aware_database_params)

    # execute query loop
    bucketfs_connection_name, schema_name = database_with_slc
    args = json.dumps(
        {
            "query_handler": {
                "factory_class": {
                    "name": "QueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContextFactory",
                    "module": "test_query_handlers.query_handler_test",
                },
                "udf": {"schema": schema_name, "name": "AAF_QUERY_HANDLER_UDF"},
                "parameter": TEST_INPUT,
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name,
                },
                "schema_name": schema_name,
            },
        }
    )
    with pytest.raises(pyexasol.ExaQueryError) as caught_exception:
        query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
        result = conn.execute(textwrap.dedent(query)).fetchall()
    assert (
        "E-AAF-4: Error occurred while calling the query handler."
        in caught_exception.value.message
        and "The following child contexts were not released"
        in caught_exception.value.message
    )


def test_query_loop_integration_with_one_iteration_with_not_released_temporary_object(
    database_with_slc, backend_aware_database_params
):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries
    # into its own session, for easier retrieval of the audit log
    conn = pyexasol.connect(**backend_aware_database_params)

    # execute query loop
    bucketfs_connection_name, schema_name = database_with_slc
    args = json.dumps(
        {
            "query_handler": {
                "factory_class": {
                    "name": "QueryHandlerWithOneIterationWithNotReleasedTemporaryObjectFactory",
                    "module": "test_query_handlers.query_handler_test",
                },
                "udf": {"schema": schema_name, "name": "AAF_QUERY_HANDLER_UDF"},
                "parameter": TEST_INPUT,
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name,
                },
                "schema_name": schema_name,
            },
        }
    )
    with pytest.raises(pyexasol.ExaQueryError) as caught_exception:
        query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
        result = conn.execute(textwrap.dedent(query)).fetchall()
    assert (
        "E-AAF-4: Error occurred while calling the query handler."
        in caught_exception.value.message
        and "The following child contexts were not released"
        in caught_exception.value.message
    )

    # get audit logs after executing query loop
    conn.execute(QUERY_FLUSH_STATS)
    audit_logs: list[tuple[str]] = conn.execute(
        textwrap.dedent(QUERY_AUDIT_LOGS)
    ).fetchmany(N_FETCHED_ROWS)
    executed_queries = [row[0] for row in audit_logs]
    table_cleanup_query = [
        query
        for query in executed_queries
        if query.startswith(f'DROP TABLE IF EXISTS "{schema_name}"."DB1_')
    ]
    for query in executed_queries:
        print("executed_query: ", query)
    assert table_cleanup_query


def test_query_loop_integration_with_two_iteration(
    database_with_slc, backend_aware_database_params
):
    # start a new db session, to isolate the EXECUTE SCRIPT and the QueryHandler queries
    # into its own session, for easier retrieval of the audit log
    conn = pyexasol.connect(**backend_aware_database_params)

    # execute query loop
    bucketfs_connection_name, schema_name = database_with_slc
    args = json.dumps(
        {
            "query_handler": {
                "factory_class": {
                    "name": "QueryHandlerTestWithTwoIterationFactory",
                    "module": "test_query_handlers.query_handler_test",
                },
                "udf": {"schema": schema_name, "name": "AAF_QUERY_HANDLER_UDF"},
                "parameter": TEST_INPUT,
            },
            "temporary_output": {
                "bucketfs_location": {
                    "directory": "directory",
                    "connection_name": bucketfs_connection_name,
                },
                "schema_name": schema_name,
            },
        }
    )
    query = f"EXECUTE SCRIPT {schema_name}.AAF_RUN_QUERY_HANDLER('{args}')"
    result = conn.execute(textwrap.dedent(query)).fetchall()

    # get audit logs after executing query loop
    conn.execute(QUERY_FLUSH_STATS)
    audit_logs: list[tuple[str]] = conn.execute(
        textwrap.dedent(QUERY_AUDIT_LOGS)
    ).fetchmany(N_FETCHED_ROWS)
    executed_queries = [row[0] for row in audit_logs]
    view_cleanup_query = [
        query
        for query in executed_queries
        if query.startswith(f'DROP VIEW IF EXISTS "{schema_name}"."DB1_')
    ]
    expected_query_list = {query.query_string for query in QUERY_LIST}
    select_queries_from_query_handler = {
        query for query in executed_queries if query in expected_query_list
    }
    # TODO build an assert which can find a list of regex as a subsequence of a list of strings,
    #  see https://kalnytskyi.com/posts/assert-str-matches-regex-in-pytest/
    # asserts
    for query in executed_queries:
        print("executed_query: ", query)
    assert (
        result[0][0] == FINAL_RESULT
        and select_queries_from_query_handler == expected_query_list
        and view_cleanup_query
    ), f"Not all required queries where executed {executed_queries}"
