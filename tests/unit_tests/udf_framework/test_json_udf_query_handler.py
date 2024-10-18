import json
import pytest

from json import JSONDecodeError
from typing import Union

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.json_udf_query_handler import JSONQueryHandler, JSONType
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.python_query_result import PythonQueryResult
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.udf_framework.json_udf_query_handler_factory import JsonUDFQueryHandler


class ConstructorTestJSONQueryHandler(JSONQueryHandler):

    def __init__(self, parameter: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[JSONType]]:
        raise AssertionError("Should not be called")

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        raise AssertionError("Should not be called")


def test_constructor_valid_json(top_level_query_handler_context):
    parameter = {
        "test_key": "test_value"
    }
    json_str_parameter = json.dumps(parameter)
    query_handler = JsonUDFQueryHandler(
        parameter=json_str_parameter,
        query_handler_context=top_level_query_handler_context,
        wrapped_json_query_handler_class=ConstructorTestJSONQueryHandler
    )


def test_constructor_invalid_json(top_level_query_handler_context):
    with pytest.raises(JSONDecodeError):
        query_handler = JsonUDFQueryHandler(
            parameter="'abc'='ced'",
            query_handler_context=top_level_query_handler_context,
            wrapped_json_query_handler_class=ConstructorTestJSONQueryHandler
        )


class StartReturnParameterTestJSONQueryHandler(JSONQueryHandler):

    def __init__(self, parameter: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameter)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        raise AssertionError("Should not be called")


def test_start_return_parameter(top_level_query_handler_context):
    parameter = {
        "test_key": "test_value"
    }
    json_str_parameter = json.dumps(parameter)
    query_handler = JsonUDFQueryHandler(
        parameter=json_str_parameter,
        query_handler_context=top_level_query_handler_context,
        wrapped_json_query_handler_class=StartReturnParameterTestJSONQueryHandler
    )
    result = query_handler.start()
    assert isinstance(result, Finish) and result.result == json_str_parameter


class HandleQueryResultCheckQueryResultTestJSONQueryHandler(JSONQueryHandler):

    def __init__(self, parameter: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[JSONType]]:
        raise AssertionError("Should not be called")

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        a = query_result.a
        return Finish[JSONType]({"a": a})


def test_handle_query_result_check_query_result(top_level_query_handler_context):
    parameter = {
        "test_key": "test_value"
    }
    json_str_parameter = json.dumps(parameter)
    query_handler = JsonUDFQueryHandler(
        parameter=json_str_parameter,
        query_handler_context=top_level_query_handler_context,
        wrapped_json_query_handler_class=HandleQueryResultCheckQueryResultTestJSONQueryHandler
    )
    result = query_handler.handle_query_result(
        PythonQueryResult(data=[(1,)],
                        columns=[Column(ColumnName("a"),
                                        ColumnType("INTEGER"))]))
    assert isinstance(result, Finish) and result.result == '{"a": 1}'
