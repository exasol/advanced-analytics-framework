from typing import List, Union
from unittest.mock import MagicMock, Mock

import pytest

from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    ResultHandlerReturnValue,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import Continue, Finish
from tests.unit_tests.sql_stage_graph.stage_graph_execution_query_handler.assert_helper import (
    assert_parent_query_handler_context_not_called,
    assert_reference_counting_bag_creation,
    assert_reference_counting_bag_not_called,
    assert_release_on_query_handler_context_for_stage,
    assert_stage_not_called,
    assert_stage_query_handler_created,
)
from tests.unit_tests.sql_stage_graph.stage_graph_execution_query_handler.state_test_setup import (
    TestSetup,
    create_execution_query_handler_state_setup,
    create_mocks_for_stage,
)


def create_diamond_setup(
    stage1_result_prototypes: List[Union[Continue, Finish, MagicMock]],
    stage2_result_prototypes: List[Union[Continue, Finish, MagicMock]],
    stage3_result_prototypes: List[Union[Continue, Finish, MagicMock]],
    stage4_result_prototypes: List[Union[Continue, Finish, MagicMock]],
) -> TestSetup:
    stage1_setup = create_mocks_for_stage(stage1_result_prototypes, stage_index=0)
    stage2_setup = create_mocks_for_stage(stage2_result_prototypes, stage_index=1)
    stage3_setup = create_mocks_for_stage(stage3_result_prototypes, stage_index=2)
    stage4_setup = create_mocks_for_stage(stage4_result_prototypes, stage_index=3)
    sql_stage_graph = SQLStageGraph(
        start_node=stage1_setup.stage,
        end_node=stage4_setup.stage,
        edges={
            (stage1_setup.stage, stage2_setup.stage),
            (stage1_setup.stage, stage3_setup.stage),
            (stage2_setup.stage, stage4_setup.stage),
            (stage3_setup.stage, stage4_setup.stage),
        },
    )
    mock_compute_dependency_order = Mock()
    mock_compute_dependency_order.return_value = [
        stage1_setup.stage,
        stage2_setup.stage,
        stage3_setup.stage,
        stage4_setup.stage,
    ]
    sql_stage_graph.compute_dependency_order = mock_compute_dependency_order
    stage_setups = [stage1_setup, stage2_setup, stage3_setup, stage4_setup]
    state_setup = create_execution_query_handler_state_setup(
        sql_stage_graph, stage_setups
    )
    return TestSetup(stage_setups=stage_setups, state_setup=state_setup)


def create_diamond_setup_with_finish() -> TestSetup:
    test_setup = create_diamond_setup(
        stage1_result_prototypes=[Finish(result=None)],
        stage2_result_prototypes=[Finish(result=None)],
        stage3_result_prototypes=[Finish(result=None)],
        stage4_result_prototypes=[Finish(result=None)],
    )
    return test_setup


def test_get_current_query_handler_diamond_return_finish_part1():
    """
    Test get_current_query_handler after the creation of a state
    with stages arranged in a diamond where the stages return directly Finish.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[List[SQLStageInputOutput], SQLStageInputOutput]:
        result = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_creation(test_setup)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=0,
        stage_inputs=[test_setup.state_setup.sql_stage_input_output],
    )
    assert_stage_not_called(test_setup, stage_index=1)
    assert_stage_not_called(test_setup, stage_index=2)
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == test_setup.stage_setups[0].train_query_handler


def test_handle_result_diamond_return_finish_part2():
    """
    Test handle_result after the creation of a state with stages arranged in a diamond
    where the stages return directly Finish, after the first call to handle_result.
    Note: The state creates for the second stage a new
          train_query_handler which gets the output of the first
          stage as input, this is important because also the third stage
          got the output of the first stage as input.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
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
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=0)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=1,
        stage_inputs=[test_setup.stage_setups[0].results[0].result],
    )
    assert_stage_not_called(test_setup, stage_index=2)
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == ResultHandlerReturnValue.CONTINUE_PROCESSING


def test_get_current_query_handler_diamond_return_finish_part3():
    """
    Test get_current_query_handler on a state
    with stages arranged in a diamond where the stages return directly Finish,
    after the first call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
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
    ) -> QueryHandler[List[SQLStageInputOutput], SQLStageInputOutput]:
        result = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_stage_not_called(test_setup, stage_index=1)
    assert_stage_not_called(test_setup, stage_index=2)
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == test_setup.stage_setups[1].train_query_handler


def test_handle_result_diamond_return_finish_part4():
    """
    Test handle_result on a state with stages arranged in a diamond
    where the stages return directly Finish, after the first call to handle_result.
    Note: The state creates for the third stage a new
          train_query_handler which gets the output of the first
          stage as input, this is important because also the second stage
          got the output of the first stage as input.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
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
            test_setup.stage_setups[1].results[0]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=1)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=2,
        stage_inputs=[test_setup.stage_setups[0].results[0].result],
    )
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == ResultHandlerReturnValue.CONTINUE_PROCESSING


def test_get_current_query_handler_diamond_return_finish_part5():
    """
    Test get_current_query_handler on a state
    with stages arranged in a diamond where the stages return directly Finish,
    after the second call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[1].results[0]
        )
        test_setup.reset_mock()
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[List[SQLStageInputOutput], SQLStageInputOutput]:
        result = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_stage_not_called(test_setup, stage_index=1)
    assert_stage_not_called(test_setup, stage_index=2)
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == test_setup.stage_setups[2].train_query_handler


def test_handle_result_diamond_return_finish_part6():
    """
    Test handle_result on a state with stages arranged in a diamond
    where the stages return directly Finish, after the second call to handle_result.
    Note: The state creates for the third stage a new
          train_query_handler which gets the output of the second
          and third stage as input,
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[1].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> ResultHandlerReturnValue:
        result = test_setup.state_setup.execution_query_handler_state.handle_result(
            test_setup.stage_setups[2].results[0]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_stage_not_called(test_setup, stage_index=1)
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=2)
    assert_stage_query_handler_created(
        test_setup,
        stage_index=3,
        stage_inputs=[
            test_setup.stage_setups[1].results[0].result,
            test_setup.stage_setups[2].results[0].result,
        ],
    )
    assert result == ResultHandlerReturnValue.CONTINUE_PROCESSING


def test_get_current_query_handler_diamond_return_finish_part7():
    """
    Test get_current_query_handler on a state
    with stages arranged in a diamond where the stages return directly Finish,
    after the third call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[1].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[2].results[0]
        )
        test_setup.reset_mock()
        return test_setup

    def act(
        test_setup: TestSetup,
    ) -> QueryHandler[List[SQLStageInputOutput], SQLStageInputOutput]:
        result = (
            test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_parent_query_handler_context_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_stage_not_called(test_setup, stage_index=1)
    assert_stage_not_called(test_setup, stage_index=2)
    assert_stage_not_called(test_setup, stage_index=3)
    assert result == test_setup.stage_setups[3].train_query_handler


def test_handle_result_diamond_return_finish_part8():
    """
    Test handle_result on a state with stages arranged in a diamond
    where the stages return directly Finish, after the third call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[1].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[2].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> ResultHandlerReturnValue:
        result = test_setup.state_setup.execution_query_handler_state.handle_result(
            test_setup.stage_setups[3].results[0]
        )
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert_reference_counting_bag_not_called(test_setup)
    assert_stage_not_called(test_setup, stage_index=0)
    assert_stage_not_called(test_setup, stage_index=1)
    assert_stage_not_called(test_setup, stage_index=2)
    assert_release_on_query_handler_context_for_stage(test_setup, stage_index=3)
    assert result == ResultHandlerReturnValue.RETURN_RESULT


def test_get_current_query_handler_diamond_return_finish_part9():
    """
    Test get_current_query_handler on a state with stages arranged in a diamond
    where the stages return directly Finish, after the forth call to handle_result.
    """

    def arrange() -> TestSetup:
        test_setup = create_diamond_setup_with_finish()
        execution_query_handler_state = (
            test_setup.state_setup.execution_query_handler_state
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[0].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[1].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[2].results[0]
        )
        execution_query_handler_state.get_current_query_handler()
        execution_query_handler_state.handle_result(
            test_setup.stage_setups[3].results[0]
        )
        test_setup.reset_mock()
        return test_setup

    test_setup = arrange()

    with pytest.raises(RuntimeError, match="No current query handler set."):
        test_setup.state_setup.execution_query_handler_state.get_current_query_handler()
