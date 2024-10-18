from typing import Union, Dict, Any
from unittest.mock import MagicMock

import pytest
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler
from exasol_advanced_analytics_framework.query_handler.result import Continue, \
    Finish
from exasol_advanced_analytics_framework.query_result.python_query_result import PythonQueryResult
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult


class TestQueryHandler(QueryHandler[Dict[str, Any], int]):
    __test__ = False

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[int]]:
        return Continue([], SelectQueryWithColumnDefinition(f'SELECT {self._parameter["a"]} as "A"',
                                                            [Column(ColumnName("A"), ColumnType("DECIMAL(12,0)"))]))

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[int]]:
        return Finish(query_result.A)


def test():
    query_handler_context = MagicMock(ScopeQueryHandlerContext)
    handler = TestQueryHandler({"a": 2}, query_handler_context)
    result = handler.start()
    if isinstance(result, Continue):
        query_result = PythonQueryResult([(2,)], result.input_query.output_columns)
        actual_result = handler.handle_query_result(query_result).result
        assert actual_result == 2
    else:
        pytest.fail()
