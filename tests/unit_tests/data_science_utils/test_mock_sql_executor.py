from exasol.analytics.sql_executor.testing.mock_result_set import MockResultSet
from exasol.analytics.sql_executor.testing.mock_sql_executor import MockSQLExecutor, ExpectedQuery


def test_no_resultset():
    expected = [ExpectedQuery("SELECT 1"), ExpectedQuery("SELECT 2")]
    executor = MockSQLExecutor(expected)
    for q in expected:
        executor.execute(q.expected_query)


def test_resultset():
    expected = [
        ExpectedQuery("SELECT 1", MockResultSet(rows=[("a",)])),
        ExpectedQuery("SELECT 2", MockResultSet(rows=[("b",)])),
    ]
    executor = MockSQLExecutor(expected)
    for q in expected:
        executor.execute(q.expected_query)
