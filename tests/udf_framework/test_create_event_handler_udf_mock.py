from tempfile import TemporaryDirectory

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor
from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContextBase
from exasol_advanced_analytics_framework.event_handler.event_handler_base \
    import EventHandlerBase
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext
from exasol_advanced_analytics_framework.event_handler.event_handler_result \
    import EventHandlerResultBase, EventHandlerResultFinished
from tests.udf_framework import mock_event_handlers


def _udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol_advanced_analytics_framework.udf_framework.\
        create_event_handler_udf import CreateEventHandlerUDF

    udf = CreateEventHandlerUDF(exa)
    def run(ctx: UDFContext):
        udf.run(ctx)


def _create_mock_data():
    meta = MockMetaData(
        script_code_wrapper_function=_udf_wrapper,
        input_type="SET",
        input_columns=[
            Column("iter_num", int, "INTEGER"),
            Column("bucketfs_connection_name", str, "VARCHAR(2000000)"),
            Column("event_handler_class_name", str, "VARCHAR(2000000)"),
            Column("event_handler_module", str, "VARCHAR(2000000)"),
            Column("event_handler_parameters", str, "VARCHAR(2000000)"),
         ],
        output_type="EMIT",
        output_columns=[
            Column("outputs", str, "VARCHAR(2000000)")
        ]
    )
    return meta


def test_event_handler_udf_with_one_iteration():
    executor = UDFMockExecutor()
    meta = _create_mock_data()

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
            assert len(result_row) == 4
            is_finished = result_row[2][0]
            final_result = result_row[3][0]
            assert is_finished == "True"
            assert final_result == str(mock_event_handlers.FINAL_RESULT)


def test_event_handler_udf_with_multiple_iteration():
    executor = UDFMockExecutor()
    meta = _create_mock_data()

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
            assert len(result_row) == 4 + len(mock_event_handlers.QUERY_LIST)
            query_view = result_row[0][0]
            query_return = result_row[1][0]
            is_finished = result_row[2][0]
            assert is_finished == "False"
            assert query_view == \
                   "Create view \"TEST_SCHEMA\".\"TMP_VIEW\" as SELECT " \
                   "AAF_EVENT_HANDLER_UDF(1, 'bucketfs_connection', " \
                   "'MockEventHandlerWithTwoIterations');"
            assert query_return == \
                   "SELECT \"TEST_SCHEMA\".\"AAF_EVENT_HANDLER_UDF\"(0," \
                   "'bucketfs_connection') FROM \"TEST_SCHEMA\".\"TMP_VIEW\";"
            for i, query_ in enumerate(mock_event_handlers.QUERY_LIST):
                assert query_ == result_row[4+i][0]

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
            assert len(result_row) == 4
            is_finished = result_row[2][0]
            final_result = result_row[3][0]
            assert is_finished == "True"
            assert final_result == str(mock_event_handlers.FINAL_RESULT)

