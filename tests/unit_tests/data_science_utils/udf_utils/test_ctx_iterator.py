import pytest
from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


def udf_wrapper():
    from exasol_udf_mock_python.udf_context import UDFContext
    from exasol.analytics.udf.utils.iterators import ctx_iterator

    def run(ctx: UDFContext):
        iter = ctx_iterator(ctx, 3, lambda: ctx.reset())
        for b in iter:
            ctx.emit(b)


@pytest.mark.parametrize("input_size", [i for i in range(1, 150)])
def test_ctx_iterator(input_size):
    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t1", int, "INTEGER"),
                       Column("t2", float, "FLOAT"), ],
        output_type="EMITS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", float, "FLOAT")]
    )
    exa = MockExaEnvironment(meta)
    input_data = [(i, 1.0 * i) for i in range(input_size)]
    result = executor.run([Group(input_data)], exa)
    assert len(result[0]) == input_size
