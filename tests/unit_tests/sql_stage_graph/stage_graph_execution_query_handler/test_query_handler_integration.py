import dataclasses
import enum
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import List, Union, Callable, Optional, Tuple
from unittest.mock import Mock

import pytest

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.result import Finish, Continue
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import LocalFSMockBucketFSLocation
from exasol_data_science_utils_python.schema.column_builder import ColumnBuilder
from exasol_data_science_utils_python.schema.column_name_builder import ColumnNameBuilder
from exasol_data_science_utils_python.schema.column_type import ColumnType
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table_builder import TableBuilder
from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.table_name_builder import TableNameBuilder
from exasol_machine_learning_library.execution.sql_stage_graph.sql_stage_graph import SQLStageGraph
from exasol_machine_learning_library.execution.sql_stage_graph_execution.data_partition import DataPartition
from exasol_machine_learning_library.execution.sql_stage_graph_execution.dataset import Dataset
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_graph_execution_input import \
    SQLStageGraphExecutionInput
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_graph_execution_query_handler import \
    SQLStageGraphExecutionQueryHandler
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput
from exasol_machine_learning_library.execution.stage_graph.sql_stage_train_query_handler import \
    SQLStageTrainQueryHandler, SQLStageTrainQueryHandlerInput
from exasol_machine_learning_library.execution.stage_graph.stage import SQLStage

pytest_plugins = [
    "tests.fixtures.top_level_query_handler_context_fixture"
]


class StartOnlyForwardInputTestSQLStageTrainQueryHandler(SQLStageTrainQueryHandler):

    def __init__(self, parameter: SQLStageTrainQueryHandlerInput,
                 query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        return Finish[SQLStageInputOutput](self._parameter.sql_stage_inputs[0])

    def handle_query_result(self, query_result: QueryResult) \
            -> Union[Continue, Finish[SQLStageInputOutput]]:
        raise NotImplementedError()


class StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler(SQLStageTrainQueryHandler):

    def __init__(self, parameter: SQLStageTrainQueryHandlerInput,
                 query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self.stage_input_output: Optional[SQLStageInputOutput] = None
        self.input_table_like_name: Optional[str] = None

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        dataset = self._parameter.sql_stage_inputs[0].dataset
        input_table_like = dataset.data_partitions[TestDatasetPartitionName.TRAIN].table_like
        # This tests also, if temporary table names are still valid
        self.input_table_like_name = input_table_like.name.fully_qualified

        output_table_name = self._query_handler_context.get_temporary_table_name()
        self.stage_input_output = create_stage_input_output(output_table_name)
        return Finish[SQLStageInputOutput](self.stage_input_output)

    def handle_query_result(self, query_result: QueryResult) \
            -> Union[Continue, Finish[SQLStageInputOutput]]:
        raise NotImplementedError()


class HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler(SQLStageTrainQueryHandler):

    def __init__(self, parameter: SQLStageTrainQueryHandlerInput,
                 query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self.stage_input_output: Optional[SQLStageInputOutput] = None
        self.continue_result: Optional[Continue] = None
        self.query_result: Optional[QueryResult] = None

    def start(self) -> Union[Continue, Finish[SQLStageInputOutput]]:
        dataset = self._parameter.sql_stage_inputs[0].dataset
        table_like = dataset.data_partitions[TestDatasetPartitionName.TRAIN].table_like
        table_like_name = table_like.name
        table_like_columns = table_like.columns
        select_query_with_column_definition = SelectQueryWithColumnDefinition(
            f"{table_like_name.fully_qualified}",
            table_like_columns
        )
        self.continue_result = Continue(query_list=[], input_query=select_query_with_column_definition)
        return self.continue_result

    def handle_query_result(self, query_result: QueryResult) \
            -> Union[Continue, Finish[SQLStageInputOutput]]:
        self.query_result = query_result
        table_name = self._query_handler_context.get_temporary_table_name()
        self.stage_input_output = create_stage_input_output(table_name)
        return Finish[SQLStageInputOutput](self.stage_input_output)


TrainQueryHandlerFactory = Callable[
    [SQLStageTrainQueryHandlerInput, ScopeQueryHandlerContext], SQLStageTrainQueryHandler]


class TestSQLStage(SQLStage):

    def __init__(self, *,
                 index: int,
                 train_query_handler_factory: TrainQueryHandlerFactory):
        self._train_query_handler_factory = train_query_handler_factory
        self.sql_stage_train_query_handler: Optional[SQLStageTrainQueryHandler] = None
        self._index = index

    def create_train_query_handler(self, query_handler_input: SQLStageTrainQueryHandlerInput,
                                   query_handler_context: ScopeQueryHandlerContext) -> SQLStageTrainQueryHandler:
        self.sql_stage_train_query_handler = self._train_query_handler_factory(query_handler_input,
                                                                               query_handler_context)
        return self.sql_stage_train_query_handler

    def __eq__(self, other):
        if isinstance(other, TestSQLStage):
            return self._index == other._index
        else:
            return False

    def __hash__(self):
        return self._index


class TestDatasetPartitionName(enum.Enum):
    TRAIN = enum.auto()


def create_input() -> SQLStageInputOutput:
    table_name = TableNameBuilder.create("TEST_TABLE", SchemaName("TEST_SCHEMA"))
    stage_input_output = create_stage_input_output(table_name)
    return stage_input_output


def create_stage_input_output(table_name: TableName):
    identifier_column = ColumnBuilder() \
        .with_name(ColumnNameBuilder().with_name("ID").build()) \
        .with_type(ColumnType("INTEGER")).build()
    sample_column = ColumnBuilder() \
        .with_name(ColumnNameBuilder().with_name("SAMPLE").build()) \
        .with_type(ColumnType("INTEGER")).build()
    target_column = ColumnBuilder() \
        .with_name(ColumnNameBuilder().with_name("TARGET").build()) \
        .with_type(ColumnType("INTEGER")).build()
    columns = [
        identifier_column,
        sample_column,
        target_column,
    ]
    table_like = TableBuilder() \
        .with_name(table_name) \
        .with_columns(columns).build()
    data_partition = DataPartition(
        table_like=table_like,
    )
    dataset = Dataset(
        data_partitions={TestDatasetPartitionName.TRAIN: data_partition},
        target_columns=[target_column],
        sample_columns=[sample_column],
        identifier_columns=[identifier_column]
    )
    stage_input_output = SQLStageInputOutput(
        dataset=dataset
    )
    return stage_input_output


@dataclasses.dataclass
class TestSetup:
    stages: List[TestSQLStage]
    stage_input_output: SQLStageInputOutput
    child_query_handler_context: ScopeQueryHandlerContext
    query_handler: SQLStageGraphExecutionQueryHandler


def create_test_setup(*, sql_stage_graph: SQLStageGraph,
                      stages: List[TestSQLStage],
                      local_query_handler_context_without_connection: TopLevelQueryHandlerContext,
                      local_fs_mock_bucket_fs_tmp_path: PurePosixPath) -> TestSetup:
    stage_input_output = create_input()
    parameter = SQLStageGraphExecutionInput(
        sql_stage_graph=sql_stage_graph,
        result_bucketfs_location=LocalFSMockBucketFSLocation(local_fs_mock_bucket_fs_tmp_path),
        input=stage_input_output
    )
    child_query_handler_context = local_query_handler_context_without_connection.get_child_query_handler_context()
    query_handler = SQLStageGraphExecutionQueryHandler(
        parameter=parameter,
        query_handler_context=child_query_handler_context
    )
    return TestSetup(
        stages=stages,
        stage_input_output=stage_input_output,
        child_query_handler_context=child_query_handler_context,
        query_handler=query_handler
    )


def test_start_with_single_stage_with_start_only_forward_train_query_handler(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which returns a StartOnlyForwardInputTestSQLStageTrainQueryHandler.
    It expects:
        - that the dataset of the result is the dataset we initialed the SQLStageGraphExecutionQueryHandler with
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, still returns no cleanup queries
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=StartOnlyForwardInputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage1,
            edges=[]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert isinstance(result, Finish) \
           and isinstance(result.result, SQLStageInputOutput) \
           and result.result.dataset == test_setup.stage_input_output.dataset \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0

    test_setup.child_query_handler_context.release()

    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0


def test_start_with_two_stages_with_start_only_forward_train_query_handler(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with two stages which return a StartOnlyForwardInputTestSQLStageTrainQueryHandler.
    It expects:
        - that the dataset of the result is the dataset we initialized the SQLStageGraphExecutionQueryHandler with
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, still returns no cleanup queries
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=StartOnlyForwardInputTestSQLStageTrainQueryHandler)
        stage2 = TestSQLStage(
            index=2,
            train_query_handler_factory=StartOnlyForwardInputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage2,
            edges=[(stage1, stage2)]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1, stage2],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    assert isinstance(result, Finish) \
           and isinstance(result.result, SQLStageInputOutput) \
           and result.result.dataset == test_setup.stage_input_output.dataset \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0

    test_setup.child_query_handler_context.release()

    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0


@contextmanager
def not_raises(exception):
    try:
        yield
    except exception:
        raise pytest.fail("DID RAISE {0}".format(exception))


def test_start_with_single_stage_with_start_only_create_new_output_train_query_handler(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which return a StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler.
    It expects:
        - that the dataset of the result is the dataset created by the
          StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler of the single stage
        - that input_table_like_name of the StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler is not None
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup queries
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage1,
            edges=[]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_train_query_handler = test_setup.stages[0].sql_stage_train_query_handler
    assert isinstance(result, Finish) \
           and isinstance(result.result, SQLStageInputOutput) \
           and result.result.dataset != test_setup.stage_input_output.dataset \
           and isinstance(stage_1_train_query_handler, StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler) \
           and result.result.dataset == stage_1_train_query_handler.stage_input_output.dataset \
           and stage_1_train_query_handler.input_table_like_name is not None \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.dataset.data_partitions[TestDatasetPartitionName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_start_with_two_stages_with_start_only_create_new_output_train_query_handler(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with two stages which return a StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler.
    It expects:
        - that the dataset of the result is the dataset created by the
          StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler of the second stage
        - that input_table_like_name of the StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler is not None
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns a single cleanup query
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler)
        stage2 = TestSQLStage(
            index=2,
            train_query_handler_factory=StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage2,
            edges=[(stage1, stage2)]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1, stage2],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_train_query_handler = test_setup.stages[0].sql_stage_train_query_handler
    stage_2_train_query_handler = test_setup.stages[1].sql_stage_train_query_handler
    assert isinstance(result, Finish) \
           and isinstance(result.result, SQLStageInputOutput) \
           and result.result.dataset != test_setup.stage_input_output.dataset \
           and isinstance(stage_1_train_query_handler, StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler) \
           and isinstance(stage_2_train_query_handler, StartOnlyCreateNewOutputTestSQLStageTrainQueryHandler) \
           and result.result.dataset == stage_2_train_query_handler.stage_input_output.dataset \
           and stage_1_train_query_handler.input_table_like_name is not None \
           and stage_2_train_query_handler.input_table_like_name is not None \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.dataset.data_partitions[TestDatasetPartitionName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1


def test_start_with_single_stage_with_handle_query_result_create_new_output_train_query_handler_part1(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test runs an integration test for the start method of a SQLStageGraphExecutionQueryHandler
    on a SQLStageGraph with a single stage which return a HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler.
    It expects:
        - that the result of start is the same as the continue of the train query handler of the stage
        - that stage_input_output of train query handler of the stage is None
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns a single cleanup query
    """

    def arrange() -> TestSetup:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage1,
            edges=[]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    stage_1_train_query_handler = test_setup.stages[0].sql_stage_train_query_handler
    assert isinstance(stage_1_train_query_handler, HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler) and \
           result == stage_1_train_query_handler.continue_result \
           and stage_1_train_query_handler.stage_input_output is None \
           and stage_1_train_query_handler.query_result is None \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0


def test_handle_query_result_with_single_stage_with_handle_query_result_create_new_output_train_query_handler_part2(
        query_handler_context_with_local_bucketfs_and_no_connection, tmp_path):
    """
    This test uses test_start_with_single_stage_with_handle_query_result_create_new_output_train_query_handler_part1
    as setup and runs handle_query_result on the SQLStageGraphExecutionQueryHandler.
    It expects:
        - that the result of handle_query_result is the same as the stage_input_output
          of the train query handler of the stage
        - that recorded query_result in the train query handler is the same as we put into the handle_query_result call
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() directly
          after the call to start, returns no cleanup query
        - that local_query_handler_context_without_connection.cleanup_released_object_proxies() after a call to
          child_query_handler_context.release, returns a single cleanup query
    """

    def arrange() -> Tuple[TestSetup, QueryResult]:
        stage1 = TestSQLStage(
            index=1,
            train_query_handler_factory=HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler)
        sql_stage_graph = SQLStageGraph(
            start_node=stage1,
            end_node=stage1,
            edges=[]
        )
        test_setup = create_test_setup(sql_stage_graph=sql_stage_graph,
                                       stages=[stage1],
                                       local_query_handler_context_without_connection=query_handler_context_with_local_bucketfs_and_no_connection,
                                       local_fs_mock_bucket_fs_tmp_path=PurePosixPath(tmp_path))
        test_setup.query_handler.start()
        query_result: QueryResult = Mock()
        return test_setup, query_result

    def act(test_setup: TestSetup, query_result: QueryResult) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.query_handler.handle_query_result(query_result)
        return result

    test_setup, query_result = arrange()
    result = act(test_setup, query_result)

    stage_1_train_query_handler = test_setup.stages[0].sql_stage_train_query_handler
    assert isinstance(result, Finish) \
           and isinstance(stage_1_train_query_handler,
                          HandleQueryResultCreateNewOutputTestSQLStageTrainQueryHandler) and \
           result.result == stage_1_train_query_handler.stage_input_output \
           and query_result == stage_1_train_query_handler.query_result \
           and len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 0

    if isinstance(result, Finish) and isinstance(result.result, SQLStageInputOutput):
        with not_raises(Exception):
            name = result.result.dataset.data_partitions[TestDatasetPartitionName.TRAIN].table_like.name

    test_setup.child_query_handler_context.release()
    assert len(query_handler_context_with_local_bucketfs_and_no_connection.cleanup_released_object_proxies()) == 1
