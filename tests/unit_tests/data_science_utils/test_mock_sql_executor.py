from exasol_data_science_utils_python.udf_utils.testing.mock_result_set import MockResultSet
from exasol_data_science_utils_python.udf_utils.testing.mock_sql_executor import MockSQLExecutor


def test_no_resultset():
    executor = MockSQLExecutor()
    executor.execute("SELECT 1")
    executor.execute("SELECT 2")
    assert executor.queries == ["SELECT 1", "SELECT 2"]


def test_resultset():
    input_rs1 = MockResultSet(rows=[("a",)])
    input_rs2 = MockResultSet(rows=[("b",)])
    executor = MockSQLExecutor(
        result_sets=[
            input_rs1,
            input_rs2])
    rs1 = executor.execute("SELECT 1")
    rs2 = executor.execute("SELECT 2")
    assert executor.queries == ["SELECT 1", "SELECT 2"]
    assert rs1 == input_rs1
    assert rs2 == input_rs2
