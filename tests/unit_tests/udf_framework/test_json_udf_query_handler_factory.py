import json

from typing import Union

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.json_udf_query_handler import JSONQueryHandler, JSONType
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.mock_query_result import MockQueryResult
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.udf_framework.json_udf_query_handler_factory import JsonUDFQueryHandlerFactory
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler


class TestJSONQueryHandler(JSONQueryHandler):
    __test__ = False
    def __init__(self, parameters: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameters, query_handler_context)
        self._parameters = parameters

    def start(self) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameters)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameters)


class TestJsonUDFQueryHandlerFactory(JsonUDFQueryHandlerFactory):
    __test__ = False
    def __init__(self):
        super().__init__(TestJSONQueryHandler)


def test(top_level_query_handler_context):
    test_input = {"a": 1}
    json_str = json.dumps(test_input)
    query_handler = TestJsonUDFQueryHandlerFactory().create(json_str, top_level_query_handler_context)
    start_result = query_handler.start()
    handle_query_result = query_handler.handle_query_result(
        MockQueryResult(data=[(1,)],
                        columns=[Column(ColumnName("a"),
                                        ColumnType("INTEGER"))])
    )
    assert isinstance(query_handler, UDFQueryHandler) \
           and start_result.result == json_str and handle_query_result.result == json_str
