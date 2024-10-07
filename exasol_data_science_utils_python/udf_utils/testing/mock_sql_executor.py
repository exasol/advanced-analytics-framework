from typing import Optional, List

from exasol_data_science_utils_python.udf_utils.sql_executor import SQLExecutor, ResultSet
from exasol_data_science_utils_python.udf_utils.testing.mock_result_set import MockResultSet


class MockSQLExecutor(SQLExecutor):
    def __init__(self, result_sets: Optional[List[MockResultSet]] = None):
        self.result_sets = result_sets
        self.queries = []

    def execute(self, sql: str) -> ResultSet:
        self.queries.append(sql)
        if self.result_sets is None:
            return MockResultSet()
        else:
            if len(self.queries) > len(self.result_sets):
                raise RuntimeError(f"No result set found for query {sql}")
            return self.result_sets[len(self.queries) - 1]
