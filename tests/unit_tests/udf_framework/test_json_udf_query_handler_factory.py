import json

from typing import Union

from exasol.analytics.schema import (
    Column,
    ColumnType,
    ColumnName,
)

from exasol.analytics.query_handler.context.scope import     ScopeQueryHandlerContext
from exasol.analytics.query_handler.json_udf_query_handler import JSONQueryHandler, JSONType
from exasol.analytics.query_handler.result import Continue, Finish
from exasol.analytics.query_handler.query.result.python_query_result import PythonQueryResult
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.udf.json_impl import JsonUDFQueryHandlerFactory
from exasol.analytics.query_handler.udf.interface import UDFQueryHandler


class TestJSONQueryHandler(JSONQueryHandler):
    __test__ = False
    def __init__(self, parameter: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameter)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameter)


class TestJsonUDFQueryHandlerFactory(JsonUDFQueryHandlerFactory):
    __test__ = False
    def __init__(self):
        super().__init__(TestJSONQueryHandler)


def test(top_level_query_handler_context_mock):
    test_input = {"a": 1}
    json_str = json.dumps(test_input)
    query_handler = TestJsonUDFQueryHandlerFactory().create(json_str, top_level_query_handler_context_mock)
    start_result = query_handler.start()
    handle_query_result = query_handler.handle_query_result(
        PythonQueryResult(data=[(1,)],
                        columns=[Column(ColumnName("a"),
                                        ColumnType("INTEGER"))])
    )
    assert isinstance(query_handler, UDFQueryHandler) \
           and start_result.result == json_str and handle_query_result.result == json_str
