import dataclasses
from inspect import cleandoc
from typing import Optional, List, Dict, Tuple

from exasol_data_science_utils_python.udf_utils.sql_executor import SQLExecutor, ResultSet
from exasol_data_science_utils_python.udf_utils.testing.mock_result_set import MockResultSet


@dataclasses.dataclass
class ExpectedQuery:
    expected_query: str
    mock_result_set: MockResultSet


class MockSQLExecutor(SQLExecutor):
    def __init__(self, expected_queries: Optional[List[ExpectedQuery]] = None):
        self._expected_queries = expected_queries
        self._expected_query_iterator = iter(expected_queries)

    def execute(self, sql: str) -> ResultSet:
        if self._expected_queries is None:
            return MockResultSet()
        else:
            try:
                next_expected_query = next(self._expected_query_iterator)
                if next_expected_query.expected_query != sql:
                    raise RuntimeError(
                        cleandoc(
                            f"""Expected query
{next_expected_query.expected_query}
but got
{sql}"""))
            except StopIteration as e:
                raise RuntimeError(f"No result set found for query {sql}")
