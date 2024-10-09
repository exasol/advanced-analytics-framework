import json
from abc import ABC
from typing import Type, Union

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.query_handler.json_udf_query_handler import JSONQueryHandler, JSONType
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler_factory import UDFQueryHandlerFactory


class JsonUDFQueryHandler(UDFQueryHandler):

    def __init__(self, parameters: str,
                 query_handler_context: ScopeQueryHandlerContext,
                 wrapped_json_query_handler_class: Type[JSONQueryHandler]):
        super().__init__(parameters, query_handler_context)
        json_parameter = json.loads(parameters)
        self._wrapped_json_query_handler = wrapped_json_query_handler_class(
            parameters=json_parameter,
            query_handler_context=query_handler_context)

    def start(self) -> Union[Continue, Finish[str]]:
        result = self._wrapped_json_query_handler.start()
        return self._handle_result(result)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        result = self._wrapped_json_query_handler.handle_query_result(query_result)
        return self._handle_result(result)

    @staticmethod
    def _handle_result(result: Union[Continue, Finish[JSONType]]) -> Union[Continue, Finish[str]]:
        if isinstance(result, Continue):
            return result
        elif isinstance(result, Finish):
            new_result = Finish[str](json.dumps(result.result))
            return new_result
        else:
            raise ValueError("Unknown Result")


class JsonUDFQueryHandlerFactory(UDFQueryHandlerFactory, ABC):

    def __init__(self, wrapped_json_query_handler_class: Type[JSONQueryHandler]):
        self._wrapped_json_query_handler_class = wrapped_json_query_handler_class

    def create(self, parameters: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return JsonUDFQueryHandler(
            parameters=parameters,
            query_handler_context=query_handler_context,
            wrapped_json_query_handler_class=self._wrapped_json_query_handler_class)
