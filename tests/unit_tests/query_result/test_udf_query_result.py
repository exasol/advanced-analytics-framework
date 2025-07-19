from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor

DATA_SIZE = 100
FETCH_SIZE = 10
INPUT_DATA = [(i, (1.0 * i / DATA_SIZE), str(2 * i)) for i in range(1, DATA_SIZE + 1)]
INPUT_COLUMNS = [
    Column("t1", int, "INTEGER"),
    Column("t2", float, "FLOAT"),
    Column("t3", str, "VARCHAR(2000)"),
]


def test_fetch_as_dataframe_column_names():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            df = wrapper.fetch_as_dataframe(num_rows=1)
            for column in df.columns:
                ctx.emit(column)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("column", str, "VARCHAR(2000000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == [("a",), ("b",), ("c",)]


def test_fetch_as_dataframe_first_batch():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            df = wrapper.fetch_as_dataframe(num_rows=10)
            ctx.emit(df[["a", "c", "b"]])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA[:10]


def test_fetch_as_dataframe_second_batch():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            df = wrapper.fetch_as_dataframe(num_rows=10)
            df = wrapper.fetch_as_dataframe(num_rows=20)
            ctx.emit(df[["a", "c", "b"]])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA[10:30]


def test_fetch_as_dataframe_after_last_batch():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            while True:
                df = wrapper.fetch_as_dataframe(num_rows=10)
                if df is None:
                    break
                ctx.emit(df[["a", "c", "b"]])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA


def test_fetch_as_dataframe_all_rows():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            df = wrapper.fetch_as_dataframe(num_rows="all")
            ctx.emit(df[["a", "c", "b"]])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA


def test_fetch_as_dataframe_start_col():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            df = wrapper.fetch_as_dataframe(num_rows=1, start_col=1)
            ctx.emit(df[["c", "b"]])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows[0] == INPUT_DATA[0][1:]


def test_rowcount():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            ctx.emit(wrapper.rowcount())

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("rowcount", int, "INTEGER"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows[0][0] == len(INPUT_DATA)


def test_column_names():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            for column_name in wrapper.column_names():
                ctx.emit(column_name)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("column_name", str, "VARCHAR(1000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == [("a",), ("b",), ("c",)]


def test_columns():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            for column in wrapper.columns():
                ctx.emit(column.name.name, column.type.name)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("column_name", str, "VARCHAR(1000)"),
            Column("sql_type", str, "VARCHAR(1000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == [("a", "INTEGER"), ("b", "VARCHAR(2000)"), ("c", "FLOAT")]


def test_column_get_attr():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            a = wrapper.a
            b = wrapper.b
            c = wrapper.c
            ctx.emit(a, c, b)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows[0] == INPUT_DATA[0]


def test_column_get_item():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            a = wrapper["a"]
            b = wrapper["b"]
            c = wrapper["c"]
            ctx.emit(a, c, b)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows[0] == INPUT_DATA[0]


def test_column_next_get_item():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            while True:
                a = wrapper["a"]
                b = wrapper["b"]
                c = wrapper["c"]
                ctx.emit(a, c, b)
                if not wrapper.next():
                    break

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA


def test_column_next_get_attr():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            while True:
                a = wrapper.a
                b = wrapper.b
                c = wrapper.c
                ctx.emit(a, c, b)
                if not wrapper.next():
                    break

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA


def test_column_iterator():
    def udf_wrapper():
        from collections import OrderedDict

        from exasol_udf_mock_python.udf_context import UDFContext

        from exasol.analytics.query_handler.query.result.udf_query_result import (
            UDFQueryResult,
        )

        def run(ctx: UDFContext):
            wrapper = UDFQueryResult(
                ctx, exa, OrderedDict([("t1", "a"), ("t3", "b"), ("t2", "c")])
            )
            for row in wrapper:
                ctx.emit(row[0], row[2], row[1])

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=INPUT_COLUMNS,
        output_type="EMITS",
        output_columns=[
            Column("a", int, "INTEGER"),
            Column("c", float, "FLOAT"),
            Column("b", str, "VARCHAR(2000)"),
        ],
    )

    exa = MockExaEnvironment(meta)
    result = executor.run([Group(INPUT_DATA)], exa)
    assert result[0].rows == INPUT_DATA
