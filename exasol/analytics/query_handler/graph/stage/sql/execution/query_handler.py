from typing import Callable, Union

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.input import (
    SQLStageGraphExecutionInput,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    ResultHandlerReturnValue,
    SQLStageGraphExecutionQueryHandlerState,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import Continue, Finish

SQLStageGraphExecutionQueryHandlerStateFactory = Callable[
    [SQLStageGraphExecutionInput, ScopeQueryHandlerContext],
    SQLStageGraphExecutionQueryHandlerState,
]


class SQLStageGraphExecutionQueryHandler(
    QueryHandler[SQLStageGraphExecutionInput, SQLStageInputOutput]
):
    """
    Implementation of the execution graph QueryHandler. Each node of the graph contains
    its own, user provided, query handler.
    """

    def __init__(
        self,
        parameter: SQLStageGraphExecutionInput,
        query_handler_context: ScopeQueryHandlerContext,
        query_handler_state_factory: SQLStageGraphExecutionQueryHandlerStateFactory = SQLStageGraphExecutionQueryHandlerState,
    ):
        super().__init__(parameter, query_handler_context)
        self._state = query_handler_state_factory(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        # Start traversing the graph from the root node.
        result = self._run_until_continue_or_last_stage_finished()
        return result

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        """
        Here we call the handle_query_result() of the query handler at the current node.
        If this handler returns a Finish object then we proceed to the next node, if
        there is any, and start traversing the graph from there (see
        _run_until_continue_or_last_stage_finished()). Otherwise, if the returned object
        is Continue, we stay on the same node and return the result.
        """
        result = self._state.get_current_query_handler().handle_query_result(
            query_result
        )
        result_handler_return_value = self._state.handle_result(result)
        if result_handler_return_value == ResultHandlerReturnValue.RETURN_RESULT:
            return result
        elif (
            result_handler_return_value == ResultHandlerReturnValue.CONTINUE_PROCESSING
        ):
            result = self._run_until_continue_or_last_stage_finished()
        else:
            raise RuntimeError("Unknown result_handler_return_value")
        return result

    def _run_until_continue_or_last_stage_finished(
        self,
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        """
        We start traversing the graph from the current node by calling the start() on
        its query handler. If the handler returns a Continue object we will exit the
        loop and return the result. Otherwise, if the handler returns a Finish object,
        we will move to the next node and call the start() on its query handler. We will
        carry on until a Continue object is returned or the whole graph is traversed.
        """
        while True:
            handler = self._state.get_current_query_handler()
            result = handler.start()
            result_handler_return_value = self._state.handle_result(result)
            if result_handler_return_value == ResultHandlerReturnValue.RETURN_RESULT:
                return result
            elif (
                result_handler_return_value
                == ResultHandlerReturnValue.CONTINUE_PROCESSING
            ):
                pass
            else:
                raise RuntimeError("Unknown result_handler_return_value")
