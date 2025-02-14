import json
from abc import ABC
from typing import (
    Type,
    Union,
)

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.json_udf_query_handler import (
    JSONQueryHandler,
    JSONType,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)
from exasol.analytics.query_handler.udf.interface import (
    UDFQueryHandler,
    UDFQueryHandlerFactory,
)


class JsonUDFQueryHandler(UDFQueryHandler):

    def __init__(
        self,
        parameter: str,
        query_handler_context: ScopeQueryHandlerContext,
        wrapped_json_query_handler_class: Type[JSONQueryHandler],
    ):
        super().__init__(parameter, query_handler_context)
        json_parameter = json.loads(parameter)
        self._wrapped_json_query_handler = wrapped_json_query_handler_class(
            parameter=json_parameter, query_handler_context=query_handler_context
        )

    def start(self) -> Union[Continue, Finish[str]]:
        result = self._wrapped_json_query_handler.start()
        return self._handle_result(result)

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[str]]:
        result = self._wrapped_json_query_handler.handle_query_result(query_result)
        return self._handle_result(result)

    @staticmethod
    def _handle_result(
        result: Union[Continue, Finish[JSONType]],
    ) -> Union[Continue, Finish[str]]:
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

    def create(
        self, parameter: str, query_handler_context: ScopeQueryHandlerContext
    ) -> UDFQueryHandler:
        return JsonUDFQueryHandler(
            parameter=parameter,
            query_handler_context=query_handler_context,
            wrapped_json_query_handler_class=self._wrapped_json_query_handler_class,
        )
