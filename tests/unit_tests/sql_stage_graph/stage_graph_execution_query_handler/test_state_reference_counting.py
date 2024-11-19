import dataclasses
from typing import Dict, List, Union
from unittest.mock import MagicMock, Mock, call, create_autospec

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    ResultHandlerReturnValue,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.result import Continue, Finish
from tests.mock_cast import mock_cast
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
    stage1_setup = create_mocks_for_stage(stage1_result_prototypes, stage_index=1)
    stage2_setup = create_mocks_for_stage(stage2_result_prototypes, stage_index=2)
    stage3_setup = create_mocks_for_stage(stage3_result_prototypes, stage_index=3)
    stage4_setup = create_mocks_for_stage(stage4_result_prototypes, stage_index=4)
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


@dataclasses.dataclass
class ReferenceCountingSetup:
    test_setup: TestSetup
    object_proxy_dict: Dict[ObjectProxy, int]


def create_diamond_setup_with_finish_with_last_stage_returning_new_result() -> (
    ReferenceCountingSetup
):
    test_setup = create_diamond_setup(
        stage1_result_prototypes=[Finish(result=None)],
        stage2_result_prototypes=[Finish(result=None)],
        stage3_result_prototypes=[Finish(result=None)],
        stage4_result_prototypes=[Finish(result=None)],
    )
    stage1_object_proxy = create_autospec(ObjectProxy)
    stage2_object_proxy = create_autospec(ObjectProxy)
    stage4_object_proxy = create_autospec(ObjectProxy)
    test_setup.stage_setups[0].results[0].result = stage1_object_proxy
    test_setup.stage_setups[1].results[0].result = stage2_object_proxy
    test_setup.stage_setups[2].results[0].result = stage1_object_proxy
    test_setup.stage_setups[3].results[0].result = stage4_object_proxy
    object_proxy_dict = equip_reference_counting_bag_with_logic(test_setup)
    return ReferenceCountingSetup(test_setup, object_proxy_dict)


def create_diamond_setup_with_finish_with_last_stage_returning_existing_result() -> (
    ReferenceCountingSetup
):
    test_setup = create_diamond_setup(
        stage1_result_prototypes=[Finish(result=None)],
        stage2_result_prototypes=[Finish(result=None)],
        stage3_result_prototypes=[Finish(result=None)],
        stage4_result_prototypes=[Finish(result=None)],
    )
    stage1_object_proxy = create_autospec(ObjectProxy)
    test_setup.stage_setups[0].results[0].result = stage1_object_proxy
    test_setup.stage_setups[1].results[0].result = stage1_object_proxy
    test_setup.stage_setups[2].results[0].result = stage1_object_proxy
    test_setup.stage_setups[3].results[0].result = stage1_object_proxy
    object_proxy_dict = equip_reference_counting_bag_with_logic(test_setup)
    return ReferenceCountingSetup(test_setup, object_proxy_dict)


def equip_reference_counting_bag_with_logic(test_setup) -> Dict[ObjectProxy, int]:
    object_proxy_dict = dict()

    def side_effect_contains(object_proxy):
        return object_proxy in object_proxy_dict

    def side_effect_add(object_proxy):
        if object_proxy not in object_proxy_dict:
            object_proxy_dict[object_proxy] = 1
        else:
            object_proxy_dict[object_proxy] += 1

    def side_effect_remove(object_proxy):
        if object_proxy not in object_proxy_dict:
            raise AssertionError("Should not happen")
        else:
            object_proxy_dict[object_proxy] -= 1
        if object_proxy_dict[object_proxy] == 0:
            del object_proxy_dict[object_proxy]

    def side_effect_transfer_back(object_proxy):
        del object_proxy_dict[object_proxy]

    mock_cast(
        test_setup.state_setup.reference_counting_bag_mock_setup.bag.__contains__
    ).side_effect = side_effect_contains
    mock_cast(
        test_setup.state_setup.reference_counting_bag_mock_setup.bag.add
    ).side_effect = side_effect_add
    mock_cast(
        test_setup.state_setup.reference_counting_bag_mock_setup.bag.remove
    ).side_effect = side_effect_remove
    mock_cast(
        test_setup.state_setup.reference_counting_bag_mock_setup.bag.transfer_back_to_parent_query_handler_context
    ).side_effect = side_effect_transfer_back
    return object_proxy_dict


def assert_transfer_from_child_to_parent_query_handler_context(
    ref_count_setup: ReferenceCountingSetup, stage_index: int
):
    mock_cast(
        ref_count_setup.test_setup.stage_setups[
            stage_index
        ].child_query_handler_context.transfer_object_to
    ).assert_called_once_with(
        ref_count_setup.test_setup.stage_setups[stage_index].results[0].result,
        ref_count_setup.test_setup.state_setup.parent_query_handler_context,
    )


def assert_no_transfer_from_child_to_parent_query_handler_context(
    ref_count_setup: ReferenceCountingSetup, stage_index: int
):
    mock_cast(
        ref_count_setup.test_setup.stage_setups[
            stage_index
        ].child_query_handler_context.transfer_object_to
    ).assert_not_called()


def test_handle_result_diamond_return_finish_new_result_part1():
    """
    This test calls handle_result with the result for the first stage
    on a diamond stage graph where the last stage returns a new result.
    """

    def arrange() -> ReferenceCountingSetup:
        ref_count_setup = (
            create_diamond_setup_with_finish_with_last_stage_returning_new_result()
        )
        ref_count_setup.test_setup.reset_mock()
        return ref_count_setup

    def act(ref_count_setup: ReferenceCountingSetup) -> ResultHandlerReturnValue:
        result = ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[0].results[0]
        )
        return result

    ref_count_setup = arrange()
    result = act(ref_count_setup)

    assert (
        ref_count_setup.test_setup.state_setup.reference_counting_bag_mock_setup.bag.mock_calls
        == [
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[0].results[0].result
            ),
            call.add(ref_count_setup.test_setup.stage_setups[0].results[0].result),
            call.add(ref_count_setup.test_setup.stage_setups[0].results[0].result),
        ]
    )
    assert_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 0)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 1)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 2)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 3)


def test_handle_result_diamond_return_finish_new_result_part2():
    """
    This test use test_handle_result_diamond_return_finish_new_result_part1 as setup and
    calls handle_result with the result of the second stage.
    """

    def arrange() -> ReferenceCountingSetup:
        ref_count_setup = (
            create_diamond_setup_with_finish_with_last_stage_returning_new_result()
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[0].results[0]
        )
        ref_count_setup.test_setup.reset_mock()
        return ref_count_setup

    def act(ref_count_setup: ReferenceCountingSetup) -> ResultHandlerReturnValue:
        result = ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[1].results[0]
        )
        return result

    ref_count_setup = arrange()
    result = act(ref_count_setup)

    assert (
        ref_count_setup.test_setup.state_setup.reference_counting_bag_mock_setup.bag.mock_calls
        == [
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[1].results[0].result
            ),
            call.add(ref_count_setup.test_setup.stage_setups[1].results[0].result),
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[0].results[0].result
            ),
            call.remove(ref_count_setup.test_setup.stage_setups[0].results[0].result),
        ]
    )
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 0)
    assert_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 1)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 2)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 3)


def test_handle_result_diamond_return_finish_new_result_part3():
    """
    This test use test_handle_result_diamond_return_finish_new_result_part2 as setup and
    calls handle_result with the result of the third stage.
    """

    def arrange() -> ReferenceCountingSetup:
        ref_count_setup = (
            create_diamond_setup_with_finish_with_last_stage_returning_new_result()
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[0].results[0]
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[1].results[0]
        )
        ref_count_setup.test_setup.reset_mock()
        return ref_count_setup

    def act(ref_count_setup: ReferenceCountingSetup) -> ResultHandlerReturnValue:
        result = ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[2].results[0]
        )
        return result

    ref_count_setup = arrange()
    result = act(ref_count_setup)

    assert (
        ref_count_setup.test_setup.state_setup.reference_counting_bag_mock_setup.bag.mock_calls
        == [
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[0].results[0].result
            ),
            call.add(ref_count_setup.test_setup.stage_setups[0].results[0].result),
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[0].results[0].result
            ),
            call.remove(ref_count_setup.test_setup.stage_setups[0].results[0].result),
        ]
    )
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 0)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 1)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 2)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 3)


def test_handle_result_diamond_return_finish_new_result_part4():
    """
    This test use test_handle_result_diamond_return_finish_new_result_part3 as setup and
    calls handle_result with the result of the forth stage.
    """

    def arrange() -> ReferenceCountingSetup:
        ref_count_setup = (
            create_diamond_setup_with_finish_with_last_stage_returning_new_result()
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[0].results[0]
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[1].results[0]
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[2].results[0]
        )
        ref_count_setup.test_setup.reset_mock()
        return ref_count_setup

    def act(ref_count_setup: ReferenceCountingSetup) -> ResultHandlerReturnValue:
        result = ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[3].results[0]
        )
        return result

    ref_count_setup = arrange()
    result = act(ref_count_setup)
    print(ref_count_setup.object_proxy_dict)
    ref_count_setup.test_setup.state_setup.reference_counting_bag_mock_setup.bag.assert_has_calls(
        [
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[1].results[0].result
            ),
            call.remove(ref_count_setup.test_setup.stage_setups[1].results[0].result),
            call.__contains__(
                ref_count_setup.test_setup.stage_setups[0].results[0].result
            ),
            call.remove(ref_count_setup.test_setup.stage_setups[0].results[0].result),
        ]
    )
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 0)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 1)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 2)
    assert_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 3)


def test_handle_result_diamond_return_finish_existing_result():
    """
    This test uses a execution_query_handler_state with diamond stage graph where all stages return the same.
    It then setups the execution_query_handler_state with handle_result calls for the first three stages.
    The test then call handle_result for the last stage and expects a call to
    transfer_back_to_parent_query_handler_context on the reference_counting_bag to transfer back the existing
    result.

    """

    def arrange() -> ReferenceCountingSetup:
        ref_count_setup = (
            create_diamond_setup_with_finish_with_last_stage_returning_existing_result()
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[0].results[0]
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[1].results[0]
        )
        ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[2].results[0]
        )
        ref_count_setup.test_setup.reset_mock()
        return ref_count_setup

    def act(ref_count_setup: ReferenceCountingSetup) -> ResultHandlerReturnValue:
        result = ref_count_setup.test_setup.state_setup.execution_query_handler_state.handle_result(
            ref_count_setup.test_setup.stage_setups[3].results[0]
        )
        return result

    ref_count_setup = arrange()
    result = act(ref_count_setup)

    state_setup = ref_count_setup.test_setup.state_setup
    mock_cast(
        state_setup.reference_counting_bag_mock_setup.bag.transfer_back_to_parent_query_handler_context
    ).assert_called_once_with(
        ref_count_setup.test_setup.stage_setups[0].results[0].result
    )
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 0)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 1)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 2)
    assert_no_transfer_from_child_to_parent_query_handler_context(ref_count_setup, 3)
