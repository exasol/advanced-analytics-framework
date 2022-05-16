from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


DATA_SIZE = 100
FETCH_SIZE = 10
INPUT_DATA = [(i, (1.0 * i / DATA_SIZE), (2.0 * i))
              for i in range(1, DATA_SIZE+1)]


def udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol_advanced_analytics_framework.event_context.udf_event_context \
        import UDFEventContext
    from collections import OrderedDict

    def run(ctx: UDFContext):
        wrapper = UDFEventContext(
            ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")]))
        df = wrapper.fetch_as_dataframe(10)
        assert wrapper.column_names() == ["t1", "t2", "t3"]
        assert wrapper.rowcount() == 100

        ctx.emit(df)


def test_udf_event_context():
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
                        Column("t2", float, "FLOAT"),
                        Column("t3", float, "FLOAT")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    for i, group in enumerate(result):
        result_row = group.rows
        assert len(result_row) == FETCH_SIZE
        for row in group.rows:
            assert row[2] <= 1.0
