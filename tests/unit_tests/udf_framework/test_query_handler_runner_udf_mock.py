import re
from tempfile import TemporaryDirectory

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor

from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf import QueryHandlerStatus
from tests.unit_tests.udf_framework import mock_query_handlers
from tests.unit_tests.udf_framework.mock_query_handlers import TEST_CONNECTION
from tests.utils.test_utils import pytest_regex

TEMPORARY_NAME_PREFIX = "temporary_name_prefix"

BUCKETFS_DIRECTORY = "directory"

BUCKETFS_CONNECTION_NAME = "bucketfs_connection"


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


def test_query_handler_udf_with_one_iteration():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/query_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

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
        result = executor.run([Group([input_data])], exa)
        rows = [row[0] for row in result[0].rows]
        expected_rows = [None, None, QueryHandlerStatus.FINISHED.name, mock_query_handlers.FINAL_RESULT]
        assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_with_not_released_child_query_handler_context():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/query_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

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
        result = executor.run([Group([input_data])], exa)
        rows = [row[0] for row in result[0].rows]
        expected_rows = [None,
                         None,
                         QueryHandlerStatus.ERROR.name,
                         pytest_regex(r".*The following child contexts were not released:*", re.DOTALL)]
        assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_with_not_released_temporary_object():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/query_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

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
        result = executor.run([Group([input_data])], exa)
        rows = [row[0] for row in result[0].rows]
        expected_rows = [None,
                         None,
                         QueryHandlerStatus.ERROR.name,
                         pytest_regex(r".*The following child contexts were not released.*", re.DOTALL),
                         'DROP TABLE IF EXISTS "temp_schema"."temporary_name_prefix_2_1";']
        assert rows == expected_rows


def test_query_handler_udf_with_one_iteration_and_temp_table():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/query_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

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
        result = executor.run([Group([input_data])], exa)
        rows = [row[0] for row in result[0].rows]
        table_cleanup_query = 'DROP TABLE IF EXISTS "temp_schema"."temporary_name_prefix_1";'
        expected_rows = [None, None, QueryHandlerStatus.FINISHED.name, mock_query_handlers.FINAL_RESULT,
                         table_cleanup_query]
        assert rows == expected_rows


def test_query_handler_udf_with_two_iteration(tmp_path):
    executor = UDFMockExecutor()
    meta = create_mock_data()

    bucketfs_connection = Connection(address=f"file://{tmp_path}/query_handler")
    exa = MockExaEnvironment(
        metadata=meta,
        connections={BUCKETFS_CONNECTION_NAME: bucketfs_connection})

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

    prev_state_exist = _is_state_exist(0, bucketfs_connection)
    current_state_exist = _is_state_exist(1, bucketfs_connection)
    assert prev_state_exist == False and current_state_exist == True

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
        connections={BUCKETFS_CONNECTION_NAME: bucketfs_connection})

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


def test_query_handler_udf_using_connection():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/query_handler")
        test_connection = Connection(address=f"test_connection",
                                     user="test_connection_user",
                                     password="test_connection_pwd")

        exa = MockExaEnvironment(
            metadata=meta,
            connections={
                "bucketfs_connection": bucketfs_connection,
                TEST_CONNECTION: test_connection}
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
        result = executor.run([Group([input_data])], exa)
        rows = [row[0] for row in result[0].rows]
        expected_rows = [
            None, None, QueryHandlerStatus.FINISHED.name,
            f"{TEST_CONNECTION},{test_connection.address},{test_connection.user},{test_connection.password}"
        ]
        assert rows == expected_rows


def _is_state_exist(
        iter_num: int,
        model_connection: Connection) -> bool:
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=model_connection.address,
        user=model_connection.user,
        pwd=model_connection.password)
    bucketfs_path = f"{BUCKETFS_DIRECTORY}/{TEMPORARY_NAME_PREFIX}/state/"
    state_file = f"{str(iter_num)}.pkl"
    files = bucketfs_location.list_files_in_bucketfs(bucketfs_path)
    return state_file in files
