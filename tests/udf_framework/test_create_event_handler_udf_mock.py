from pathlib import PurePosixPath
from tempfile import TemporaryDirectory
import pytest
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor
from tests.udf_framework import mock_event_handlers


def _udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol_advanced_analytics_framework.udf_framework.\
        create_event_handler_udf import CreateEventHandlerUDF

    udf = CreateEventHandlerUDF(exa)
    def run(ctx: UDFContext):
        udf.run(ctx)


def create_mock_data():
    meta = MockMetaData(
        script_code_wrapper_function=_udf_wrapper,
        input_type="SET",
        input_columns=[
            Column("1", int, "INTEGER"),  # iter_num
            Column("2", str, "VARCHAR(2000000)"),  # bucketfs_connection_name
            Column("3", str, "VARCHAR(2000000)"),  # event_handler_class_name
            Column("4", str, "VARCHAR(2000000)"),  # event_handler_module
            Column("5", str, "VARCHAR(2000000)"),  # event_handler_parameters
         ],
        output_type="EMITS",
        output_columns=[
            Column("outputs", str, "VARCHAR(2000000)")
        ],
        is_variadic_input=True
    )
    return meta


def test_event_handler_udf_with_one_iteration():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/event_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

        input_data = (
            0,
            "bucketfs_connection",
            "MockEventHandlerWithOneIteration",
            "tests.udf_framework.mock_event_handlers",
            "{}"
        )
        result = executor.run([Group([input_data])], exa)
        for i, group in enumerate(result):
            result_row = group.rows
            is_finished = result_row[2][0]
            final_result = result_row[3][0]
            assert len(result_row) == 4 \
                   and is_finished == "True" \
                   and final_result == str(mock_event_handlers.FINAL_RESULT)


def test_event_handler_udf_with_two_iteration():
    executor = UDFMockExecutor()
    meta = create_mock_data()

    with TemporaryDirectory() as path:
        bucketfs_connection = Connection(address=f"file://{path}/event_handler")
        exa = MockExaEnvironment(
            metadata=meta,
            connections={"bucketfs_connection": bucketfs_connection})

        input_data = (
            0,
            "bucketfs_connection",
            "MockEventHandlerWithTwoIterations",
            "tests.udf_framework.mock_event_handlers",
            "{}"
        )
        result = executor.run([Group([input_data])], exa)
        for i, group in enumerate(result):
            result_row = group.rows
            query_view = result_row[0][0]
            query_return = result_row[1][0]
            is_finished = result_row[2][0]
            assert len(result_row) == 4 + len(mock_event_handlers.QUERY_LIST) \
                   and is_finished == "False" \
                   and query_view == "Create view \"TEST_SCHEMA\".\"TMP_VIEW\" " \
                                     "as SELECT a, table1.b, c FROM table1, " \
                                     "table2 WHERE table1.b=table2.b;" \
                   and query_return == "SELECT \"TEST_SCHEMA\"." \
                                       "\"AAF_EVENT_HANDLER_UDF\"(1," \
                                       "'bucketfs_connection',\"a\",\"b\") " \
                                       "FROM \"TEST_SCHEMA\".\"TMP_VIEW\";" \
                   and set(mock_event_handlers.QUERY_LIST) == set(
                            list(map(lambda x: x[0], result_row[4+i:])))

        prev_state_exist = _is_state_exist(
            0, "MockEventHandlerWithTwoIterations", bucketfs_connection)
        current_state_exist = _is_state_exist(
            1, "MockEventHandlerWithTwoIterations", bucketfs_connection)
        assert not prev_state_exist and current_state_exist

        input_data = (
            1,
            "bucketfs_connection",
            "MockEventHandlerWithTwoIterations",
            "",
            ""
        )
        result = executor.run([Group([input_data])], exa)
        for i, group in enumerate(result):
            result_row = group.rows
            is_finished = result_row[2][0]
            final_result = result_row[3][0]
            assert len(result_row) == 4 \
                   and is_finished == "True" \
                   and final_result == str(mock_event_handlers.FINAL_RESULT)


def _is_state_exist(
        iter_num: int, event_handler_class: str,
        model_connection: Connection) -> bool:
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=model_connection.address,
        user=model_connection.user,
        pwd=model_connection.password)
    bucketfs_path = f"{event_handler_class}_{str(iter_num)}.pkl"

    files = bucketfs_location.list_files_in_bucketfs("")
    return bucketfs_path in files


