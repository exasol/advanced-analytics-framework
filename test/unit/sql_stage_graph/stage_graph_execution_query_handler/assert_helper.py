from typing import List

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandlerInput,
)
from test.unit.sql_stage_graph.stage_graph_execution_query_handler.state_test_setup import (
    TestSetup,
)
from test.utils.mock_cast import mock_cast


def assert_reference_counting_bag_not_called(test_setup: TestSetup):
    reference_counting_bag_mock_setup = (
        test_setup.state_setup.reference_counting_bag_mock_setup
    )
    assert reference_counting_bag_mock_setup.bag.mock_calls == []


def assert_reference_counting_bag_creation(test_setup: TestSetup):
    parent_query_handler_context = test_setup.state_setup.parent_query_handler_context
    reference_counting_bag_mock_setup = (
        test_setup.state_setup.reference_counting_bag_mock_setup
    )

    reference_counting_bag_mock_setup.factory.assert_called_once_with(
        parent_query_handler_context
    )
    assert reference_counting_bag_mock_setup.bag.mock_calls == []


def assert_parent_query_handler_context_not_called(test_setup: TestSetup):
    parent_query_handler_context = test_setup.state_setup.parent_query_handler_context
    assert parent_query_handler_context.mock_calls == []


def assert_stage_not_called(test_setup: TestSetup, *, stage_index: int):
    stage_setup = test_setup.stage_setups[stage_index]
    mock_cast(stage_setup.stage.create_query_handler).assert_not_called()
    assert stage_setup.query_handler.mock_calls == []
    assert stage_setup.child_query_handler_context.mock_calls == []


def assert_stage_query_handler_created(
    test_setup: TestSetup, *, stage_index: int, stage_inputs: list[SQLStageInputOutput]
):
    stage_setup = test_setup.stage_setups[stage_index]
    mock_cast(
        test_setup.state_setup.result_bucketfs_location.joinpath
    ).assert_called_once_with(str(stage_index))
    mock_cast(
        test_setup.state_setup.parent_query_handler_context.get_child_query_handler_context
    ).assert_called_once()
    result_bucketfs_location = test_setup.stage_setups[
        stage_index
    ].result_bucketfs_location
    stage_input = SQLStageQueryHandlerInput(
        result_bucketfs_location=result_bucketfs_location, sql_stage_inputs=stage_inputs
    )
    mock_cast(stage_setup.stage.create_query_handler).assert_called_once_with(
        stage_input, stage_setup.child_query_handler_context
    )
    assert stage_setup.query_handler.mock_calls == []
    assert stage_setup.child_query_handler_context.mock_calls == []


def assert_release_on_query_handler_context_for_stage(
    test_setup: TestSetup, *, stage_index: int
):
    stage_setup = test_setup.stage_setups[stage_index]
    assert stage_setup.query_handler.mock_calls == []
    mock_cast(stage_setup.child_query_handler_context.release).assert_called_once()
    mock_cast(stage_setup.stage.create_query_handler).assert_not_called()
