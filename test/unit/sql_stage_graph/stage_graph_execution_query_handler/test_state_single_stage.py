from test.unit.sql_stage_graph.stage_graph_execution_query_handler.assert_helper import (
    assert_parent_query_handler_context_not_called,
    assert_reference_counting_bag_creation,
    assert_reference_counting_bag_not_called,
    assert_release_on_query_handler_context_for_stage,
    assert_stage_not_called,
    assert_stage_query_handler_created,
)
from test.unit.sql_stage_graph.stage_graph_execution_query_handler.state_test_setup import (
    TestSetup,
    create_execution_query_handler_state_setup,
    create_mocks_for_stage,
)
from typing import (
    List,
    Union,
)
from unittest.mock import MagicMock

import pytest

from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    ResultHandlerReturnValue,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)


def create_single_stage_setup(
    result_prototypes: list[Union[Continue, Finish, MagicMock]],
) -> TestSetup:
    stage_setup = create_mocks_for_stage(result_prototypes, stage_index=0)
    sql_stage_graph = SQLStageGraph(
        start_node=stage_setup.stage, end_node=stage_setup.stage, edges=set()
    )
    stage_setups = [stage_setup]
    state_setup = create_execution_query_handler_state_setup(
        sql_stage_graph, stage_setups
    )
    return TestSetup(stage_setups=stage_setups, state_setup=state_setup)


def test_get_current_query_handler_single_stage_after_init():
    """
    Test get_current_query_handler after the creation of a state with a single stage
    which directly returns Finish.
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(result_prototypes=[Finish(result=None)])
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[list[SQLStageInputOutput], SQLStageInputOutput]:
        current_query_handler = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return current_query_handler

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_creation(test_setup)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=0,
        stage_inputs=[test_setup.state_setup.sql_stage_input_output],
    )
    assert test_setup.stage_setups[0].query_handler == result


def test_handle_result_single_stage_return_finish():
    """
    Test handle_result after the creation of the state with a single stage
    which directly returns Finish and a single call to get_current_query_handler
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(result_prototypes=[Finish(result=None)])
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> ResultHandlerReturnValue:
        result = test_setup.state_setup.execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=0)
    assert result == ResultHandlerReturnValue.RETURN_RESULT


def test_get_current_query_handler_single_stage_return_finish_after_finish():
    """
    Test get_current_query_handler after the single stage was finished and fail.
    """

    test_setup = create_single_stage_setup(result_prototypes=[Finish(result=None)])
    execution_query_handler_state = test_setup.state_setup.execution_query_handler_state
    execution_query_handler_state.get_current_query_handler()
    execution_query_handler_state.handle_result(test_setup.stage_setups[0].results[0])
    test_setup.reset_mock()

    with pytest.raises(RuntimeError, match="No current query handler set."):
        execution_query_handler_state.get_current_query_handler()


def test_handle_result_single_stage_return_finish_after_finish():
    """
    Test handle_result after the single stage was finished and fail.
    """

    test_setup = create_single_stage_setup(result_prototypes=[Finish(result=None)])
    execution_query_handler_state = test_setup.state_setup.execution_query_handler_state
    execution_query_handler_state.get_current_query_handler()
    execution_query_handler_state.handle_result(test_setup.stage_setups[0].results[0])
    test_setup.reset_mock()

    with pytest.raises(RuntimeError, match="No current query handler set."):
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )


def test_get_current_query_handler_single_stage_return_continue_finish():
    """
    Test get_current_query_handler after the creation of a state with a single stage
    which returns first a Continue and after that a Finish.
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(
            result_prototypes=[
                Continue(query_list=None, input_query=None),
                Finish(result=None),
            ]
        )
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[list[SQLStageInputOutput], SQLStageInputOutput]:
        current_query_handler = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return current_query_handler

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_creation(test_setup)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=0,
        stage_inputs=[test_setup.state_setup.sql_stage_input_output],
    )
    assert test_setup.stage_setups[0].query_handler == result


def test_handle_result_single_stage_return_continue_finish_part1():
    """
    Test handle_result after the creation of a state with a single stage
    which returns first a Continue and after that a Finish and
    a single call to get_current_query_handler
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(
            result_prototypes=[
                Continue(query_list=None, input_query=None),
                Finish(result=None),
            ]
        )
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> ResultHandlerReturnValue:
        result = test_setup.state_setup.execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert result == ResultHandlerReturnValue.RETURN_RESULT


def test_get_current_query_handler_single_stage_return_continue_finish_part2():
    """
    Test get_current_query_handler on a state with a single stage
    which returns first a Continue and after that a Finish,
    after the first call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(
            result_prototypes=[
                Continue(query_list=None, input_query=None),
                Finish(result=None),
            ]
        )
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        test_setup.reset_mock()
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[list[SQLStageInputOutput], SQLStageInputOutput]:
        current_query_handler = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return current_query_handler

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert result == test_setup.stage_setups[0].query_handler


def test_handle_result_single_stage_return_continue_finish_part3():
    """
    Test handle_result on a state with a single stage
    which returns first a Continue and after that a Finish,
    after the first call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_single_stage_setup(
            result_prototypes=[
                Continue(query_list=None, input_query=None),
                Finish(result=None),
            ]
        )
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> ResultHandlerReturnValue:
        result = test_setup.state_setup.execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[1]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=0)
    assert result == ResultHandlerReturnValue.RETURN_RESULT
