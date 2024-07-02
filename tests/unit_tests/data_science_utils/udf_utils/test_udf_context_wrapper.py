from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


def udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from collections import OrderedDict
    from exasol_data_science_utils_python.udf_utils.udf_context_wrapper import UDFContextWrapper
    import numpy as np

    def run(ctx: UDFContext):
        wrapper = UDFContextWrapper(ctx, OrderedDict([("t2", "a"), ("t1", "b")]))
        df = wrapper.get_dataframe(10)
        assert len(df) == 10
        assert list(df.columns) == ["a", "b"]
        assert list(df.dtypes) == [np.float64, np.int64]

        wrapper.reset()
        df = wrapper.get_dataframe(10, start_col=1)
        assert len(df) == 10
        assert list(df.columns) == ["b"]
        assert list(df.dtypes) == [np.int64]

        wrapper.reset()
        wrapper = UDFContextWrapper(ctx, OrderedDict([("t3", "d"), ("t2", "c"), ]), start_col=1)
        df = wrapper.get_dataframe(10)
        assert len(df) == 10
        assert list(df.columns) == ["d", "c"]
        assert list(df.dtypes) == [np.float64, np.float64]
        assert all(df["c"] < 1.0)
        assert all(np.logical_or(df["d"] >= 1.0, df["d"] == 0.0))


        wrapper.reset()
        df = wrapper.get_dataframe(10, start_col=1)
        assert len(df) == 10
        assert list(df.columns) == ["c"]
        assert list(df.dtypes) == [np.float64]
        assert all(df["c"] < 1.0)


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
        output_type="EMITS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", float, "FLOAT")]
    )
    exa = MockExaEnvironment(meta)
    input_data = [(i, (1.0 * i) / 105, (1.0 * i)) for i in range(105)]
    result = executor.run([Group(input_data)], exa)
