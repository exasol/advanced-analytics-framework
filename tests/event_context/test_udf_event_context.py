from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


DATA_SIZE = 100
INPUT_DATA = input_data = [(i, (1.0 * i / DATA_SIZE), (2.0 * i))
                           for i in range(DATA_SIZE)]


def udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol_advanced_analytics_framework.event_context.udf_event_context \
        import UDFEventContext
    from collections import OrderedDict

    def run(ctx: UDFContext):
        wrapper = UDFEventContext(
            ctx, exa, OrderedDict([("t2", "a"), ("t1", "b")]))
        df = wrapper.fetch_as_dataframe(10)
        assert len(df) == 10
        assert list(df.columns) == ["a", "b"]
        assert wrapper.column_names() == ["t1", "t2", "t3"]
        assert wrapper.rowcount() == 100

        wrapper = UDFEventContext(
            ctx, exa, OrderedDict([("t3", "d"), ("t2", "c"), ]), start_col=1)
        df = wrapper.fetch_as_dataframe(10)
        assert len(df) == 10
        assert list(df.columns) == ["d", "c"]
        assert all(df["c"] < 1.0)
        assert wrapper.column_names() == ["t1", "t2", "t3"]
        assert wrapper.rowcount() == 100


def test_partial_fit_iterator():
    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[
            Column("t1", int, "INTEGER"),
            Column("t2", float, "FLOAT"),
            Column("t3", float, "FLOAT"),
        ],
        output_type="EMIT",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", float, "FLOAT")]
    )
    exa = MockExaEnvironment(meta)
    executor.run([Group(INPUT_DATA)], exa)
