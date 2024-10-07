import json
import pytest
import re

from typing import Any, Dict

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor

from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf import (
    QueryHandlerStatus,
    create_bucketfs_location_from_conn_object,
)
from tests.unit_tests.udf_framework import mock_query_handlers
from tests.unit_tests.udf_framework.mock_query_handlers import TEST_CONNECTION
from tests.utils.test_utils import pytest_regex


TEMPORARY_NAME_PREFIX = "temporary_name_prefix"

BUCKETFS_DIRECTORY = "directory"

BUCKETFS_CONNECTION_NAME = "bucketfs_connection"


def to_json_str(**kwargs):
    return json.dumps(kwargs)


def udf_mock_connection(user=None, password=None, **kwargs) -> Connection:
    """
    For MountedBucket provide kwargs backend="mounted", and base_path.
    """
    return Connection(
        address=to_json_str(**kwargs),
        user=to_json_str(username=user) if user else "{}",
        password=to_json_str(password=password) if password else "{}",
    )


@pytest.fixture
def query_handler_bfs_connection(tmp_path):
    path = tmp_path / "query_handler"
    path.mkdir()
    return udf_mock_connection(
        backend="mounted",
        base_path=f"{path}",
    )

def create_mocked_exa_env(bfs_connection, connections: Dict[str, Any] = {}):
    meta = create_mock_data()
    connections[BUCKETFS_CONNECTION_NAME] = bfs_connection
    return MockExaEnvironment(metadata=meta, connections=connections)


@pytest.fixture
def mocked_exa_env(query_handler_bfs_connection):
    return create_mocked_exa_env(query_handler_bfs_connection)


def _udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol_advanced_analytics_framework.udf_framework. \
        query_handler_runner_udf import QueryHandlerRunnerUDF

    udf = QueryHandlerRunnerUDF(exa)

    def run(ctx: UDFContext):
        udf.run(ctx)


def create_mock_data():
    meta = MockMetaData(
        script_code_wrapper_function=_udf_wrapper,
        input_type="SET",
        input_columns=[
            Column("0", int, "INTEGER"),  # iter_num
            Column("1", str, "VARCHAR(2000000)"),  # temporary_bfs_location_conn
            Column("2", str, "VARCHAR(2000000)"),  # temporary_bfs_location_directory
            Column("3", str, "VARCHAR(2000000)"),  # temporary_name_prefix
            Column("4", str, "VARCHAR(2000000)"),  # temporary_schema_name
            Column("5", str, "VARCHAR(2000000)"),  # python_class_name
            Column("6", str, "VARCHAR(2000000)"),  # python_class_module
            Column("7", str, "VARCHAR(2000000)"),  # parameters
        ],
        output_type="EMITS",
        output_columns=[
            Column("outputs", str, "VARCHAR(2000000)")
        ],
        is_variadic_input=True
    )
    return meta


def test_query_handler_udf_with_one_iteration(mocked_exa_env):
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "MockQueryHandlerWithOneIterationFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        mock_query_handlers.TEST_INPUT
    )
    result = UDFMockExecutor().run([Group([input_data])], mocked_exa_env)
    rows = [row[0] for row in result[0].rows]
    expected_rows = [None, None, QueryHandlerStatus.FINISHED.name, mock_query_handlers.FINAL_RESULT]
    assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_with_not_released_child_query_handler_context(mocked_exa_env):
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContextFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        "{}"
    )
    result = UDFMockExecutor().run([Group([input_data])], mocked_exa_env)
    rows = [row[0] for row in result[0].rows]
    expected_rows = [None,
                     None,
                     QueryHandlerStatus.ERROR.name,
                     pytest_regex(r".*The following child contexts were not released:*", re.DOTALL)]
    assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_with_not_released_temporary_object(mocked_exa_env):
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObjectFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        "{}"
    )
    result = UDFMockExecutor().run([Group([input_data])], mocked_exa_env)
    rows = [row[0] for row in result[0].rows]
    expected_rows = [None,
                     None,
                     QueryHandlerStatus.ERROR.name,
                     pytest_regex(r".*The following child contexts were not released.*", re.DOTALL),
                     'DROP TABLE IF EXISTS "temp_schema"."temporary_name_prefix_2_1";']
    assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_and_temp_table(mocked_exa_env):
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "QueryHandlerTestWithOneIterationAndTempTableFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        "{}"
    )
    result = UDFMockExecutor().run([Group([input_data])], mocked_exa_env)
    rows = [row[0] for row in result[0].rows]
    table_cleanup_query = 'DROP TABLE IF EXISTS "temp_schema"."temporary_name_prefix_1";'
    expected_rows = [None, None, QueryHandlerStatus.FINISHED.name, mock_query_handlers.FINAL_RESULT,
                     table_cleanup_query]
    assert rows == expected_rows


def test_query_handler_udf_with_two_iteration(query_handler_bfs_connection):
    def state_file_exists(iteration: int) -> bool:
        bucketfs_location = create_bucketfs_location_from_conn_object(query_handler_bfs_connection)
        bucketfs_path = f"{BUCKETFS_DIRECTORY}/{TEMPORARY_NAME_PREFIX}/state"
        state_file = f"{str(iteration)}.pkl"
        return (bucketfs_location / bucketfs_path / state_file).exists()

    exa = create_mocked_exa_env(query_handler_bfs_connection)
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "MockQueryHandlerWithTwoIterationsFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        "{}"
    )
    executor = UDFMockExecutor()
    result = executor.run([Group([input_data])], exa)
    rows = [row[0] for row in result[0].rows]
    expected_return_query_view = 'CREATE VIEW "temp_schema"."temporary_name_prefix_2_1" AS ' \
                                 'SELECT a, table1.b, c ' \
                                 'FROM table1, table2 ' \
                                 'WHERE table1.b=table2.b;'
    return_query = 'SELECT "TEST_SCHEMA"."AAF_QUERY_HANDLER_UDF"(' \
                   '1,' \
                   "'bucketfs_connection','directory','temporary_name_prefix'," \
                   '"a","b") ' \
                   'FROM "temp_schema"."temporary_name_prefix_2_1";'
    expected_rows = [expected_return_query_view, return_query, QueryHandlerStatus.CONTINUE.name, "{}"] + \
                    [query.query_string for query in mock_query_handlers.QUERY_LIST]
    assert rows == expected_rows

    previous = 0
    current = 1
    assert not state_file_exists(previous) and state_file_exists(current)

    exa = MockExaEnvironment(
        metadata=MockMetaData(
            script_code_wrapper_function=_udf_wrapper,
            input_type="SET",
            input_columns=[
                Column("0", int, "INTEGER"),  # iter_num
                Column("1", str, "VARCHAR(2000000)"),  # temporary_bfs_location_conn
                Column("2", str, "VARCHAR(2000000)"),  # temporary_bfs_location_directory
                Column("3", str, "VARCHAR(2000000)"),  # temporary_name_prefix
                Column("4", int, "INTEGER"),  # column a of the input query
                Column("5", int, "INTEGER"),  # column b of the input query
            ], output_type="EMITS",
            output_columns=[
                Column("outputs", str, "VARCHAR(2000000)")
            ],
            is_variadic_input=True),
        connections={BUCKETFS_CONNECTION_NAME: query_handler_bfs_connection})

    input_data = (
        1,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        1,
        2
    )
    result = executor.run([Group([input_data])], exa)
    rows = [row[0] for row in result[0].rows]
    cleanup_return_query_view = 'DROP VIEW IF EXISTS "temp_schema"."temporary_name_prefix_2_1";'
    expected_rows = [None, None, QueryHandlerStatus.FINISHED.name, mock_query_handlers.FINAL_RESULT,
                     cleanup_return_query_view]
    assert rows == expected_rows


def test_query_handler_udf_using_connection(query_handler_bfs_connection):
    test_connection = udf_mock_connection(
        address="test_connection",
        user="test_connection_user",
        password="test_connection_pwd",
    )
    exa = create_mocked_exa_env(
        query_handler_bfs_connection,
        { TEST_CONNECTION: test_connection },
    )
    input_data = (
        0,
        BUCKETFS_CONNECTION_NAME,
        BUCKETFS_DIRECTORY,
        TEMPORARY_NAME_PREFIX,
        "temp_schema",
        "MockQueryHandlerUsingConnectionFactory",
        "tests.unit_tests.udf_framework.mock_query_handlers",
        "{}"
    )
    result = UDFMockExecutor().run([Group([input_data])], exa)
    rows = [row[0] for row in result[0].rows]
    expected_rows = [
        None, None, QueryHandlerStatus.FINISHED.name,
        ",".join([
            TEST_CONNECTION,
            test_connection.address,
            test_connection.user,
            test_connection.password,
        ])
    ]
    assert rows == expected_rows
