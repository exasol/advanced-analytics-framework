import dataclasses
from typing import List, Union
from unittest.mock import MagicMock, Mock, create_autospec

from exasol_bucketfs_utils_python.abstract_bucketfs_location import (
    AbstractBucketFSLocation,
)

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.input import (
    SQLStageGraphExecutionInput,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.object_proxy_reference_counting_bag import (
    ObjectProxyReferenceCountingBag,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    SQLStageGraphExecutionQueryHandlerState,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage import SQLStage
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandler,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import Continue, Finish
from tests.mock_cast import mock_cast

MockScopeQueryHandlerContext = Union[ScopeQueryHandlerContext, MagicMock]
MockSQLStageTrainQueryHandler = Union[SQLStageQueryHandler, MagicMock]
MockQueryHandlerResult = Union[Continue, Finish, MagicMock]
MockSQLStageInputOutput = Union[SQLStageInputOutput, MagicMock]
MockSQLStage = Union[SQLStage, MagicMock]
MockQueryResult = Union[QueryResult, MagicMock]
MockSQLStageGraphExecutionInput = Union[SQLStageGraphExecutionInput, MagicMock]
MockObjectProxyReferenceCountingBag = Union[ObjectProxyReferenceCountingBag, MagicMock]
MockObjectProxyReferenceCountingBagFactory = Union[
    ObjectProxyReferenceCountingBag, Mock
]
MockBucketFSLocation = Union[AbstractBucketFSLocation, MagicMock]


@dataclasses.dataclass
class StageSetup:
    index: int
    child_query_handler_context: MockScopeQueryHandlerContext
    train_query_handler: MockSQLStageTrainQueryHandler
    stage: MockSQLStage
    results: List[MockQueryHandlerResult]
    result_bucketfs_location: MockBucketFSLocation

    def reset_mock(self):
        self.child_query_handler_context.reset_mock()
        self.train_query_handler.reset_mock()
        self.stage.reset_mock()
        self.result_bucketfs_location.reset_mock()
        for result in self.results:
            result.reset_mock()


@dataclasses.dataclass
class ReferenceCountingBagSetup:
    bag: MockObjectProxyReferenceCountingBag
    factory: MockObjectProxyReferenceCountingBagFactory

    def reset_mock(self):
        self.bag.reset_mock()
        self.factory.reset_mock()


@dataclasses.dataclass
class ExecutionQueryHandlerStateSetup:
    reference_counting_bag_mock_setup: ReferenceCountingBagSetup
    parent_query_handler_context: MockScopeQueryHandlerContext
    execution_query_handler_state: SQLStageGraphExecutionQueryHandlerState
    sql_stage_input_output: MockSQLStageInputOutput
    result_bucketfs_location: MockBucketFSLocation

    def reset_mock(self):
        self.sql_stage_input_output.reset_mock()
        self.parent_query_handler_context.reset_mock()
        self.reference_counting_bag_mock_setup.reset_mock()
        self.result_bucketfs_location.reset_mock()


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    stage_setups: List[StageSetup]
    state_setup: ExecutionQueryHandlerStateSetup

    def reset_mock(self):
        self.state_setup.reset_mock()
        for stage_setup in self.stage_setups:
            stage_setup.reset_mock()


def create_execution_query_handler_state_setup(
    sql_stage_graph: SQLStageGraph, stage_setups: List[StageSetup]
) -> ExecutionQueryHandlerStateSetup:
    child_scoped_query_handler_contexts = [
        stage_setup.child_query_handler_context for stage_setup in stage_setups
    ]
    scoped_query_handler_context = create_mock_query_handler_context(
        child_scoped_query_handler_contexts
    )
    sql_stage_input_output: MockSQLStageInputOutput = create_autospec(
        SQLStageInputOutput
    )
    mock_result_bucketfs_location: MockBucketFSLocation = create_autospec(
        AbstractBucketFSLocation
    )
    stage_result_bucketfs_locations = [
        stage_setup.result_bucketfs_location for stage_setup in stage_setups
    ]
    mock_cast(mock_result_bucketfs_location.joinpath).side_effect = (
        stage_result_bucketfs_locations
    )
    parameter = SQLStageGraphExecutionInput(
        input=sql_stage_input_output,
        sql_stage_graph=sql_stage_graph,
        result_bucketfs_location=mock_result_bucketfs_location,
    )
    reference_counting_bag_mock_setup = create_reference_counting_bag_mock_setup()
    execution_query_handler_state = SQLStageGraphExecutionQueryHandlerState(
        parameter=parameter,
        query_handler_context=scoped_query_handler_context,
        reference_counting_bag_factory=reference_counting_bag_mock_setup.factory,
    )
    return ExecutionQueryHandlerStateSetup(
        reference_counting_bag_mock_setup=reference_counting_bag_mock_setup,
        parent_query_handler_context=scoped_query_handler_context,
        sql_stage_input_output=sql_stage_input_output,
        execution_query_handler_state=execution_query_handler_state,
        result_bucketfs_location=mock_result_bucketfs_location,
    )


def create_reference_counting_bag_mock_setup() -> ReferenceCountingBagSetup:
    reference_counting_bag: MockObjectProxyReferenceCountingBag = create_autospec(
        ObjectProxyReferenceCountingBag
    )
    reference_counting_bag_factory: MockObjectProxyReferenceCountingBagFactory = Mock()
    reference_counting_bag_factory.return_value = reference_counting_bag
    return ReferenceCountingBagSetup(
        bag=reference_counting_bag, factory=reference_counting_bag_factory
    )


def create_mock_query_handler_context(
    child_scoped_query_handler_contexts: List[ScopeQueryHandlerContext],
) -> MockScopeQueryHandlerContext:
    scoped_query_handler_context: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    scoped_query_handler_context.get_child_query_handler_context.side_effect = (
        child_scoped_query_handler_contexts
    )
    return scoped_query_handler_context


def create_mocks_for_stage(
    result_prototypes: List[Union[Finish, Continue]], *, stage_index: int
) -> StageSetup:
    child_scoped_query_handler_context: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    sql_stage: MockSQLStage = create_autospec(SQLStage)
    sql_stage.__hash__.return_value = stage_index
    result: List[MockQueryHandlerResult] = [
        create_autospec(result_prototype) for result_prototype in result_prototypes
    ]
    train_query_handler: MockSQLStageTrainQueryHandler = create_autospec(QueryHandler)
    sql_stage.create_train_query_handler.return_value = train_query_handler
    mock_result_bucketfs_location: MockBucketFSLocation = create_autospec(
        AbstractBucketFSLocation
    )
    return StageSetup(
        index=stage_index,
        child_query_handler_context=child_scoped_query_handler_context,
        train_query_handler=train_query_handler,
        stage=sql_stage,
        results=result,
        result_bucketfs_location=mock_result_bucketfs_location,
    )
