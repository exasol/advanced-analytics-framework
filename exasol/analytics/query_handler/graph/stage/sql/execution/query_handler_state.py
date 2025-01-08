import enum
from typing import Callable, DefaultDict, List, Optional, Union

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.find_object_proxies import (
    find_object_proxies,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.input import (
    SQLStageGraphExecutionInput,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counting_bag import (
    ObjectProxyReferenceCountingBag,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage import SQLStage
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandlerInput, SQLStageQueryHandler
)
from exasol.analytics.query_handler.result import Continue, Finish
from exasol.analytics.utils.errors import UninitializedAttributeError


class ResultHandlerReturnValue(enum.Enum):
    RETURN_RESULT = enum.auto()
    CONTINUE_PROCESSING = enum.auto()


class SQLStageGraphExecutionQueryHandlerState:
    def __init__(
        self,
        parameter: SQLStageGraphExecutionInput,
        query_handler_context: ScopeQueryHandlerContext,
        reference_counting_bag_factory: Callable[
            [ScopeQueryHandlerContext], ObjectProxyReferenceCountingBag
        ] = ObjectProxyReferenceCountingBag,
    ):
        self._query_handler_context = query_handler_context
        self._sql_stage_graph = parameter.sql_stage_graph
        self._result_bucketfs_location = parameter.result_bucketfs_location
        self._reference_counting_bag = reference_counting_bag_factory(
            query_handler_context
        )
        self._stage_inputs_map = DefaultDict[SQLStage, List[SQLStageInputOutput]](list)
        self._stages_in_execution_order = (
            self._sql_stage_graph.compute_dependency_order()
        )
        self._current_stage_index = 0
        self._current_stage: Optional[SQLStage] = self._stages_in_execution_order[
            self._current_stage_index
        ]
        self._stage_inputs_map[self._current_stage].append(parameter.input)
        self._current_query_handler: Optional[SQLStageQueryHandler] = None
        self._current_qh_context: Optional[ScopeQueryHandlerContext] = None
        self._create_current_query_handler()

    def get_current_query_handler(
        self,
    ) -> SQLStageQueryHandler:
        value = self._current_query_handler
        if value is None:
            raise RuntimeError("No current query handler set.")
        return value

    @property
    def _checked_current_qh_context(self) -> ScopeQueryHandlerContext:
        value = self._current_qh_context
        if value is None:
            raise UninitializedAttributeError(
                "Current query handler context is undefined."
            )
        return value

    @property
    def _checked_current_stage(self) -> SQLStage:
        value = self._current_stage
        if value is None:
            raise UninitializedAttributeError("Current stage is None.")
        return value

    def handle_result(
        self, result: Union[Continue, Finish[SQLStageInputOutput]]
    ) -> ResultHandlerReturnValue:
        """
        Here we look at the provided last result obtained at the current node.

        If it is encapsulated in a Finish object we then either save the result so that
        it can be used as an input at the next node, or take it as the final output in
        case this node is the last one. In the former case we move to the next node and
        return CONTINUE_PROCESSING. In the latter case we return RETURN_RESULT.

        If the result is encapsulated in a Continue object we return RETURN_RESULT.
        """
        # check if current query handler is set
        self.get_current_query_handler()
        if isinstance(result, Finish):
            return self._handle_finished_result(result)
        elif isinstance(result, Continue):
            return ResultHandlerReturnValue.RETURN_RESULT
        else:
            raise RuntimeError("Unkown result type")

    def _handle_finished_result(
        self, result: Finish[SQLStageInputOutput]
    ) -> ResultHandlerReturnValue:
        """
        We are done with the current node.
        If there are successors, then we need to add the final output from this node to the
        list of inputs of all the successors.
        """
        if self._is_not_last_stage():
            self._add_result_to_successors(result.result)
        else:
            self._transfer_ownership_of_result_to_query_result_handler(result)
        self._remove_current_stage_inputs()
        return self._try_to_move_to_next_stage()

    def _try_to_move_to_next_stage(self) -> ResultHandlerReturnValue:
        self._checked_current_qh_context.release()
        if self._is_not_last_stage():
            self._move_to_next_stage()
            return ResultHandlerReturnValue.CONTINUE_PROCESSING
        else:
            self.invalidate()
            return ResultHandlerReturnValue.RETURN_RESULT

    def invalidate(self):
        self._current_stage = None
        self._current_query_handler = None
        self._current_qh_context = None

    def _is_not_last_stage(self):
        return self._current_stage_index < len(self._stages_in_execution_order) - 1

    def _move_to_next_stage(self):
        self._current_stage_index += 1
        self._current_stage = self._stages_in_execution_order[self._current_stage_index]
        self._create_current_query_handler()

    def _create_current_query_handler(self):
        stage_inputs = self._stage_inputs_map[self._current_stage]
        self._current_qh_context = (
            self._query_handler_context.get_child_query_handler_context()
        )
        result_bucketfs_location = self._result_bucketfs_location.joinpath(
            str(self._current_stage_index)
        )
        stage_input = SQLStageQueryHandlerInput(
            result_bucketfs_location=result_bucketfs_location,
            sql_stage_inputs=stage_inputs,
        )
        self._current_query_handler = self._checked_current_stage.create_query_handler(
            stage_input, self._current_qh_context
        )

    def _add_result_to_successors(self, result: SQLStageInputOutput):
        successors = self._sql_stage_graph.successors(self._checked_current_stage)
        if len(successors) == 0:
            raise RuntimeError("Programming error")
        self._add_result_to_inputs_of_successors(result, successors)
        self._add_result_to_reference_counting_bag(result, successors)

    def _add_result_to_inputs_of_successors(
        self, result: SQLStageInputOutput, successors: List[SQLStage]
    ):
        for successor in successors:
            self._stage_inputs_map[successor].append(result)

    def _add_result_to_reference_counting_bag(
        self, result: SQLStageInputOutput, successors: List[SQLStage]
    ):
        object_proxies = find_object_proxies(result)
        for object_proxy in object_proxies:
            if object_proxy not in self._reference_counting_bag:
                self._checked_current_qh_context.transfer_object_to(
                    object_proxy, self._query_handler_context
                )
            for _ in successors:
                self._reference_counting_bag.add(object_proxy)

    def _transfer_ownership_of_result_to_query_result_handler(self, result):
        object_proxies = find_object_proxies(result)
        for object_proxy in object_proxies:
            if object_proxy in self._reference_counting_bag:
                self._reference_counting_bag.transfer_back_to_parent_query_handler_context(
                    object_proxy
                )
            else:
                self._checked_current_qh_context.transfer_object_to(
                    object_proxy, self._query_handler_context
                )

    def _remove_current_stage_inputs(self):
        for stage_input in self._stage_inputs_map[self._current_stage]:
            object_proxies = find_object_proxies(stage_input)
            self._remove_object_proxies_from_reference_counting_bag(object_proxies)

    def _remove_object_proxies_from_reference_counting_bag(
        self, object_proxies: List[ObjectProxy]
    ):
        for object_proxy in object_proxies:
            if object_proxy in self._reference_counting_bag:
                self._reference_counting_bag.remove(object_proxy)
            else:
                # In case of the last stage, we already transferred some object proxies
                # to the parent_query_handler_context, such that parent query_handler
                # work on them.
                if self._is_not_last_stage():
                    raise RuntimeError("Programming Error.")
