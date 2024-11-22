import dataclasses
import difflib
from inspect import cleandoc
from typing import Dict, List, Optional, Tuple

from exasol.analytics.sql_executor.interface import ResultSet, SQLExecutor
from exasol.analytics.sql_executor.testing.mock_result_set import MockResultSet


@dataclasses.dataclass
class ExpectedQuery:
    expected_query: str
    mock_result_set: MockResultSet = MockResultSet()


class MockSQLExecutor(SQLExecutor):
    def __init__(self, expected_queries: List[ExpectedQuery] = []):
        self._expected_queries = expected_queries
        self._expected_query_iterator = iter(expected_queries)

    def execute(self, actual_query: str) -> ResultSet:
        if self._expected_queries is None:
            return MockResultSet()
        else:
            try:
                next_expected_query = next(self._expected_query_iterator)
                expected_query = next_expected_query.expected_query
                diff = "\n".join(
                    difflib.unified_diff(
                        str(expected_query).split("\n"),
                        actual_query.split("\n"),
                        "Expected Query",
                        "Actual Query",
                    )
                )
                assert expected_query == actual_query, cleandoc(
                    f"""Expected and actual query don't match:
Expected Query:
---------------
{expected_query}
...............

Actual Query:
-------------
{actual_query}
.............

Diff:
-----

{diff}
"""
                )
                return next_expected_query.mock_result_set
            except StopIteration as e:
                raise RuntimeError(f"No result set found for query {actual_query}")
