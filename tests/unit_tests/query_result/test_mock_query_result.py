from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType

from exasol_advanced_analytics_framework.query_result.mock_query_result import PythonQueryResult

DATA_SIZE = 100
FETCH_SIZE = 10
INPUT_DATA = [(i, (1.0 * i / DATA_SIZE), str(2 * i))
              for i in range(1, DATA_SIZE + 1)]
INPUT_COLUMNS = [
    Column(ColumnName("t1"), ColumnType("INTEGER")),
    Column(ColumnName("t2"), ColumnType("FLOAT")),
    Column(ColumnName("t3"), ColumnType("VARCHAR(2000)"))]


def test_fetch_as_dataframe_column_names():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    df = context.fetch_as_dataframe(num_rows=1)
    assert list(df.columns) == [column.name.name for column in INPUT_COLUMNS]


def test_fetch_as_dataframe_first_batch():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    df = context.fetch_as_dataframe(num_rows=10)
    dataframe_list_of_tuples = list(df.itertuples(index=False, name=None))
    assert dataframe_list_of_tuples == INPUT_DATA[:10]


def test_fetch_as_dataframe_second_batch():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    df = context.fetch_as_dataframe(num_rows=10)
    df = context.fetch_as_dataframe(num_rows=20)
    dataframe_list_of_tuples = list(df.itertuples(index=False, name=None))
    assert dataframe_list_of_tuples == INPUT_DATA[10:30]


def test_fetch_as_dataframe_after_last_batch():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    result = []
    while True:
        df = context.fetch_as_dataframe(num_rows=9)
        if df is None:
            break
        result += list(df.itertuples(index=False, name=None))
    assert result == INPUT_DATA


def test_fetch_as_dataframe_all_rows():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    df = context.fetch_as_dataframe(num_rows="all")
    dataframe_list_of_tuples = list(df.itertuples(index=False, name=None))
    assert dataframe_list_of_tuples == INPUT_DATA


def test_fetch_as_dataframe_start_col():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    df = context.fetch_as_dataframe(num_rows=1, start_col=1)
    dataframe_list_of_tuples = list(df.itertuples(index=False, name=None))
    assert dataframe_list_of_tuples[0] == INPUT_DATA[0][1:]


def test_rowcount():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    assert context.rowcount() == len(INPUT_DATA)


def test_column_names():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    column_names = [column.name.name for column in INPUT_COLUMNS]
    assert context.column_names() == column_names


def test_columns():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    assert context.columns() == INPUT_COLUMNS


def test_column_get_attr():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    t1 = context.t1
    t2 = context.t2
    t3 = context.t3
    assert t1 == INPUT_DATA[0][0] and t2 == INPUT_DATA[0][1] and t3 == INPUT_DATA[0][2]


def test_column_get_item():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    t1 = context["t1"]
    t2 = context["t2"]
    t3 = context["t3"]
    assert t1 == INPUT_DATA[0][0] and t2 == INPUT_DATA[0][1] and t3 == INPUT_DATA[0][2]


def test_column_next_get_item():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    result = []
    while True:
        t1 = context["t1"]
        t2 = context["t2"]
        t3 = context["t3"]
        result.append((t1, t2, t3))
        if not context.next():
            break
    assert result == INPUT_DATA


def test_column_next_get_attr():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    result = []
    while True:
        t1 = context.t1
        t2 = context.t2
        t3 = context.t3
        result.append((t1, t2, t3))
        if not context.next():
            break
    assert result == INPUT_DATA


def test_column_iterator():
    context = PythonQueryResult(data=INPUT_DATA, columns=INPUT_COLUMNS)
    result = []
    for row in context:
        result.append((row[0], row[1], row[2]))
    assert result == INPUT_DATA
