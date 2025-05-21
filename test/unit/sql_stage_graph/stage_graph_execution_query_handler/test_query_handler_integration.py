import dataclasses
import enum
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import (
    Callable,
    List,
    Optional,
    Tuple,
    Union,
)
from unittest.mock import Mock

import exasol.bucketfs as bfs
import pytest

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.context.top_level_query_handler_context import (
    TopLevelQueryHandlerContext,
)
from exasol.analytics.query_handler.graph.stage.sql.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.execution.input import (
    SQLStageGraphExecutionInput,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler import (
    SQLStageGraphExecutionQueryHandler,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    MultiDatasetSQLStageInputOutput,
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage import SQLStage
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandler,
    SQLStageQueryHandlerInput,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition
from exasol.analytics.query_handler.result import (
    Continue,
    Finish,
)
from exasol.analytics.schema import (
    DecimalColumn,
    SchemaName,
    TableBuilder,
    TableName,
    TableNameBuilder,
    VarCharColumn,
)


class StartOnlyForwardInputTestSQLStageQueryHandler(SQLStageQueryHandler):

    def __init__(
        self,
        parameter: SQLStageQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        return Finish[SQLStageInputOutput](self._parameter.sql_stage_inputs[0])

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        raise NotImplementedError()


class StartOnlyCreateNewOutputTestSQLStageQueryHandler(SQLStageQueryHandler):

    def __init__(
        self,
        parameter: SQLStageQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self.stage_input_output: Optional[SQLStageInputOutput] = None
        self.input_table_like_name: Optional[str] = None

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        datasets = self._parameter.sql_stage_inputs[0].datasets
        input_table_like = datasets[TestDatasetName.TRAIN].table_like
        # This tests also, if temporary table names are still valid
        self.input_table_like_name = input_table_like.name.fully_qualified

        output_table_name = self._query_handler_context.get_temporary_table_name()
        self.stage_input_output = create_stage_input_output(output_table_name)
        return Finish[SQLStageInputOutput](self.stage_input_output)

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        raise NotImplementedError()


class HandleQueryResultCreateNewOutputTestSQLStageQueryHandler(SQLStageQueryHandler):

    def __init__(
        self,
        parameter: SQLStageQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self.stage_input_output: Optional[SQLStageInputOutput] = None
        self.continue_result: Optional[Continue] = None
        self.query_result: Optional[QueryResult] = None

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        datasets = self._parameter.sql_stage_inputs[0].datasets
        table_like = datasets[TestDatasetName.TRAIN].table_like
        table_like_name = table_like.name
        table_like_columns = table_like.columns
        select_query_with_column_definition = SelectQueryWithColumnDefinition(
            f"{table_like_name.fully_qualified}", table_like_columns
        )
        self.continue_result = Continue(
            query_list=[], input_query=select_query_with_column_definition
        )
        return self.continue_result

    def handle_query_result(
        self, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        self.query_result = query_result
        table_name = self._query_handler_context.get_temporary_table_name()
        self.stage_input_output = create_stage_input_output(table_name)
        return Finish[SQLStageInputOutput](self.stage_input_output)


SQLStageQueryHandlerFactory = Callable[
    [SQLStageQueryHandlerInput, ScopeQueryHandlerContext], SQLStageQueryHandler
]


class TestSQLStage(SQLStage):
    __test__ = False

    def __init__(
        self, *, index: int, query_handler_factory: SQLStageQueryHandlerFactory
    ):
        self._query_handler_factory = query_handler_factory
        self.sql_stage_query_handler: Optional[SQLStageQueryHandler] = None
        self._index = index

    def create_query_handler(
        self,
        query_handler_input: SQLStageQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ) -> SQLStageQueryHandler:
        self.sql_stage_query_handler = self._query_handler_factory(
            query_handler_input, query_handler_context
        )
        return self.sql_stage_query_handler

    def __eq__(self, other):
        if isinstance(other, TestSQLStage):
            return self._index == other._index
        else:
            return False

    def __hash__(self):
        return self._index


class TestDatasetName(enum.Enum):
    __test__ = False
    TRAIN = enum.auto()


def create_input() -> SQLStageInputOutput:
    table_name = TableNameBuilder.create("TEST_TABLE", SchemaName("TEST_SCHEMA"))
    stage_input_output = create_stage_input_output(table_name)
    return stage_input_output


def create_stage_input_output(table_name: TableName):
    identifier_column = DecimalColumn.simple("ID")
    sample_column = DecimalColumn.simple("SAMPLE")
    target_column = DecimalColumn.simple("TARGET")
    columns = [
        identifier_column,
        sample_column,
        target_column,
    ]
    table_like = TableBuilder().with_name(table_name).with_columns(columns).build()
    dataset = Dataset(
        table_like=table_like,
        columns=[target_column, sample_column],
        identifier_columns=[identifier_column],
    )
    datasets = {TestDatasetName.TRAIN: dataset}
    stage_input_output = MultiDatasetSQLStageInputOutput(datasets=datasets)
    return stage_input_output


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    stages: List[TestSQLStage]
    stage_input_output: SQLStageInputOutput
    child_query_handler_context: ScopeQueryHandlerContext
    query_handler: SQLStageGraphExecutionQueryHandler


def create_test_setup(
    *,
    sql_stage_graph: SQLStageGraph,
    stages: List[TestSQLStage],
    context: TopLevelQueryHandlerContext,
    bucketfs_location: bfs.path.PathLike,
) -> TestSetup:
    stage_input_output = create_input()
    parameter = SQLStageGraphExecutionInput(
        sql_stage_graph=sql_stage_graph,
        result_bucketfs_location=bucketfs_location,
        input=stage_input_output,
    )
    child_query_handler_context = context.get_child_query_handler_context()
    query_handler = SQLStageGraphExecutionQueryHandler(
        parameter=parameter, query_handler_context=child_query_handler_context
    )
    return TestSetup(
        stages=stages,
        stage_input_output=stage_input_output,
        child_query_handler_context=child_query_handler_context,
        query_handler=query_handler,
    )


def test_start_with_single_stage_with_start_only_forward_query_handler(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which returns a StartOnlyForwardInputTestSQLStageQueryHandler.
    It expects:
        - that the dataset of the result is the dataset we initialed the SQLStageGraphExecutionQueryHandler with
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that context.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, still returns no cleanup queries
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=StartOnlyForwardInputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(start_node=stage1, end_node=stage1, edges=[])
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert (
        isinstance(result, Finish)
        and isinstance(result.result, SQLStageInputOutput)
        and result.result.datasets == test_setup.stage_input_output.datasets
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 0
    )

    test_setup.child_query_handler_context.release()

    assert (
        len(top_level_query_handler_context_mock.cleanup_released_object_proxies()) == 0
    )


def test_start_with_two_stages_with_start_only_forward_query_handler(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with two stages which return a StartOnlyForwardInputTestSQLStageQueryHandler.
    It expects:
        - that the dataset of the result is the dataset we initialized the SQLStageGraphExecutionQueryHandler with
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that context.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, still returns no cleanup queries
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=StartOnlyForwardInputTestSQLStageQueryHandler,
        )
        stage2 = TestSQLStage(
            index=2,
            query_handler_factory=StartOnlyForwardInputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(
            start_node=stage1, end_node=stage2, edges=[(stage1, stage2)]
        )
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1, stage2],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert (
        isinstance(result, Finish)
        and isinstance(result.result, SQLStageInputOutput)
        and result.result.datasets == test_setup.stage_input_output.datasets
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 0
    )

    test_setup.child_query_handler_context.release()

    assert (
        len(top_level_query_handler_context_mock.cleanup_released_object_proxies()) == 0
    )


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail(f"DID RAISE {exception}")


def test_start_with_single_stage_with_start_only_create_new_output_query_handler(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which return a StartOnlyCreateNewOutputTestSQLStageQueryHandler.
    It expects:
        - that the dataset of the result is the dataset created by the
          StartOnlyCreateNewOutputTestSQLStageQueryHandler of the single stage
        - that input_table_like_name of the StartOnlyCreateNewOutputTestSQLStageQueryHandler is not None
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that context.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(start_node=stage1, end_node=stage1, edges=[])
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_query_handler = test_setup.stages[0].sql_stage_query_handler
    assert (
        isinstance(result, Finish)
        and isinstance(result.result, SQLStageInputOutput)
        and result.result.datasets != test_setup.stage_input_output.datasets
        and isinstance(
            stage_1_query_handler,
            StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        and result.result.datasets == stage_1_query_handler.stage_input_output.datasets
        and stage_1_query_handler.input_table_like_name is not None
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 0
    )

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.datasets[TestDatasetName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert (
        len(top_level_query_handler_context_mock.cleanup_released_object_proxies()) == 1
    )


def test_start_with_two_stages_with_start_only_create_new_output_query_handler(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with two stages which return a StartOnlyCreateNewOutputTestSQLStageQueryHandler.
    It expects:
        - that the dataset of the result is the dataset created by the
          StartOnlyCreateNewOutputTestSQLStageQueryHandler of the second stage
        - that input_table_like_name of the StartOnlyCreateNewOutputTestSQLStageQueryHandler is not None
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns a single cleanup query
        - that context.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        stage2 = TestSQLStage(
            index=2,
            query_handler_factory=StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(
            start_node=stage1, end_node=stage2, edges=[(stage1, stage2)]
        )
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1, stage2],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_query_handler = test_setup.stages[0].sql_stage_query_handler
    stage_2_query_handler = test_setup.stages[1].sql_stage_query_handler
    assert (
        isinstance(result, Finish)
        and isinstance(result.result, SQLStageInputOutput)
        and result.result.datasets != test_setup.stage_input_output.datasets
        and isinstance(
            stage_1_query_handler,
            StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        and isinstance(
            stage_2_query_handler,
            StartOnlyCreateNewOutputTestSQLStageQueryHandler,
        )
        and result.result.datasets == stage_2_query_handler.stage_input_output.datasets
        and stage_1_query_handler.input_table_like_name is not None
        and stage_2_query_handler.input_table_like_name is not None
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 1
    )

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.datasets[TestDatasetName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert (
        len(top_level_query_handler_context_mock.cleanup_released_object_proxies()) == 1
    )


def test_start_with_single_stage_with_handle_query_result_create_new_output_query_handler_part1(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which return a HandleQueryResultCreateNewOutputTestSQLStageQueryHandler.
    It expects:
        - that the result of start is the same as the continue of the query handler of the stage
        - that stage_input_output of query handler of the stage is None
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=HandleQueryResultCreateNewOutputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(start_node=stage1, end_node=stage1, edges=[])
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_query_handler = test_setup.stages[0].sql_stage_query_handler
    assert (
        isinstance(
            stage_1_query_handler,
            HandleQueryResultCreateNewOutputTestSQLStageQueryHandler,
        )
        and result == stage_1_query_handler.continue_result
        and stage_1_query_handler.stage_input_output is None
        and stage_1_query_handler.query_result is None
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 0
    )


def test_handle_query_result_with_single_stage_with_handle_query_result_create_new_output_query_handler_part2(
    top_level_query_handler_context_mock,
    mocked_temporary_bucketfs_location,
):
    """
    This test uses test_start_with_single_stage_with_handle_query_result_create_new_output_query_handler_part1
    as setup and runs handle_query_result on the SQLStageGraphExecutionQueryHandler.
    It expects:
        - that the result of handle_query_result is the same as the stage_input_output
          of the query handler of the stage
        - that recorded query_result in the query handler is the same as we put into the handle_query_result call
        - that context.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup query
        - that context.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> Tuple[TestSetup, QueryResult]:
        stage1 = TestSQLStage(
            index=1,
            query_handler_factory=HandleQueryResultCreateNewOutputTestSQLStageQueryHandler,
        )
        sql_stage_graph = SQLStageGraph(start_node=stage1, end_node=stage1, edges=[])
        test_setup = create_test_setup(
            sql_stage_graph=sql_stage_graph,
            stages=[stage1],
            context=top_level_query_handler_context_mock,
            bucketfs_location=mocked_temporary_bucketfs_location,
        )
        test_setup.query_handler.start()
        query_result: QueryResult = Mock()
        return test_setup, query_result

    def act(
        test_setup: TestSetup, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.handle_query_result(query_result)
        return result

    test_setup, query_result = arrange()
    result = act(test_setup, query_result)

    stage_1_query_handler = test_setup.stages[0].sql_stage_query_handler
    assert (
        isinstance(result, Finish)
        and isinstance(
            stage_1_query_handler,
            HandleQueryResultCreateNewOutputTestSQLStageQueryHandler,
        )
        and result.result == stage_1_query_handler.stage_input_output
        and query_result == stage_1_query_handler.query_result
        and len(top_level_query_handler_context_mock.cleanup_released_object_proxies())
        == 0
    )

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.datasets[TestDatasetName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert (
        len(top_level_query_handler_context_mock.cleanup_released_object_proxies()) == 1
    )
