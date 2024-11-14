from typing import Union, Callable

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult

from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_graph_execution_input import \
    SQLStageGraphExecutionInput
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_graph_execution_query_handler_state import \
    SQLStageGraphExecutionQueryHandlerState, ResultHandlerReturnValue
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput

SQLStageGraphExecutionQueryHandlerStateFactory = \
    Callable[
        [SQLStageGraphExecutionInput, ScopeQueryHandlerContext],
        SQLStageGraphExecutionQueryHandlerState]


class SQLStageGraphExecutionQueryHandler(QueryHandler[SQLStageGraphExecutionInput, SQLStageInputOutput]):
    def __init__(self,
                 parameter: SQLStageGraphExecutionInput,
                 query_handler_context: ScopeQueryHandlerContext,
                 query_handler_state_factory: SQLStageGraphExecutionQueryHandlerStateFactory =
                 SQLStageGraphExecutionQueryHandlerState):
        super().__init__(parameter, query_handler_context)
        self._state = query_handler_state_factory(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = self._run_until_continue_or_last_stage_finished()
        return result

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = self._state.get_current_query_handler().handle_query_result(query_result)
        result_handler_return_value = self._state.handle_result(result)
        if result_handler_return_value == ResultHandlerReturnValue.RETURN_RESULT:
            return result
        elif result_handler_return_value == ResultHandlerReturnValue.CONTINUE_PROCESSING:
            result = self._run_until_continue_or_last_stage_finished()
        else:
            raise RuntimeError("Unknown result_handler_return_value")
        return result

    def _run_until_continue_or_last_stage_finished(self) \
            -> Union[Continue, Finish[SQLStageInputOutput]]:
        while True:
            handler = self._state.get_current_query_handler()
            result = handler.start()
            result_handler_return_value = self._state.handle_result(result)
            if result_handler_return_value == ResultHandlerReturnValue.RETURN_RESULT:
                return result
            elif result_handler_return_value == ResultHandlerReturnValue.CONTINUE_PROCESSING:
                pass
            else:
                raise RuntimeError("Unknown result_handler_return_value")
