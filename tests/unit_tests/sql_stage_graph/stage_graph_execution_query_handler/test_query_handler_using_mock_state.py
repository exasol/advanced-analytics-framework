import dataclasses
from typing import List, Tuple, Union
from unittest.mock import MagicMock, Mock, call, create_autospec

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.execution.input import (
    SQLStageGraphExecutionInput,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler import (
    SQLStageGraphExecutionQueryHandler,
    SQLStageGraphExecutionQueryHandlerStateFactory,
)
from exasol.analytics.query_handler.graph.stage.sql.execution.query_handler_state import (
    ResultHandlerReturnValue,
    SQLStageGraphExecutionQueryHandlerState,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandler,
)
from exasol.analytics.query_handler.query.result.interface import QueryResult
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result import Continue, Finish
from tests.utils.mock_cast import mock_cast

MockSQLStageGraphExecutionQueryHandlerState = Union[
    SQLStageGraphExecutionQueryHandlerState, MagicMock
]
MockScopeQueryHandlerContext = Union[ScopeQueryHandlerContext, MagicMock]
MockSQLStageQueryHandler = Union[SQLStageQueryHandler, MagicMock]
MockQueryHandlerResult = Union[Continue, Finish, MagicMock]
MockSQLStageGraphExecutionInput = Union[SQLStageGraphExecutionInput, MagicMock]
MockSQLStageInputOutput = Union[SQLStageInputOutput, MagicMock]
MockQueryResult = Union[QueryResult, MagicMock]
MockSQLStageGraphExecutionQueryHandlerStateFactory = Union[
    SQLStageGraphExecutionQueryHandlerStateFactory, Mock
]


@dataclasses.dataclass
class SQLStageQueryHandlerMockSetup:
    results: List[MockQueryHandlerResult]
    query_handler: MockSQLStageQueryHandler

    def reset_mock(self):
        self.query_handler.reset_mock()
        for result in self.results:
            result.reset_mock()


@dataclasses.dataclass
class SQLStageQueryHandlerSetupDefinition:
    result_prototypes: List[Union[Continue, Finish]]

    def create_mock_setup(self) -> SQLStageQueryHandlerMockSetup:
        results: List[MockQueryHandlerResult] = [
            create_autospec(result_prototype)
            for result_prototype in self.result_prototypes
        ]
        query_handler: MockSQLStageQueryHandler = create_autospec(
            QueryHandler
        )
        query_handler.start.side_effect = [results[0]]
        query_handler.handle_query_result.side_effect = results[1:]
        return SQLStageQueryHandlerMockSetup(results, query_handler)


@dataclasses.dataclass
class StateMockSetup:
    query_handler_mock_setups: List[SQLStageQueryHandlerMockSetup]
    state: MockSQLStageGraphExecutionQueryHandlerState

    def reset_mock(self):
        self.state.reset_mock()
        for query_handler_mock_setup in self.query_handler_mock_setups:
            query_handler_mock_setup.reset_mock()


@dataclasses.dataclass
class StateSetupDefinition:
    query_handler_setup_definitions: List[SQLStageQueryHandlerSetupDefinition]

    def create_mock_setup(self) -> StateMockSetup:
        query_handler_mock_setups = [
            query_handler_setup_definition.create_mock_setup()
            for query_handler_setup_definition in self.query_handler_setup_definitions
        ]
        state: MockSQLStageGraphExecutionQueryHandlerState = create_autospec(
            SQLStageGraphExecutionQueryHandlerState
        )
        query_handlers = [
            query_handler_mock_setup.query_handler
            for query_handler_mock_setup in query_handler_mock_setups
            for _ in query_handler_mock_setup.results
        ]
        state.get_current_query_handler.side_effect = query_handlers
        result_handler_return_values = [
            self._create_result_handler_return_value(result)
            for query_handler_mock_setup in query_handler_mock_setups
            for result in query_handler_mock_setup.results
        ]
        result_handler_return_values[-1] = ResultHandlerReturnValue.RETURN_RESULT
        state.handle_result.side_effect = result_handler_return_values
        return StateMockSetup(
            query_handler_mock_setups=query_handler_mock_setups, state=state
        )

    @staticmethod
    def _create_result_handler_return_value(
        result: MockQueryHandlerResult,
    ) -> ResultHandlerReturnValue:
        if isinstance(result, Continue):
            return ResultHandlerReturnValue.RETURN_RESULT
        elif isinstance(result, Finish):
            return ResultHandlerReturnValue.CONTINUE_PROCESSING
        else:
            raise RuntimeError("Unknown QueryHandlerResult")


@dataclasses.dataclass
class TestSetup:
    __test__ = False
    execution_query_handler: SQLStageGraphExecutionQueryHandler
    mock_state_factory: MockSQLStageGraphExecutionQueryHandlerStateFactory
    mock_execution_input: MockSQLStageGraphExecutionInput
    mock_scope_query_handler_context: MockScopeQueryHandlerContext
    state_mock_setup: StateMockSetup

    def reset_mock(self):
        self.mock_execution_input.reset_mock()
        self.mock_scope_query_handler_context.reset_mock()
        self.mock_state_factory.reset_mock()
        self.state_mock_setup.reset_mock()


def create_test_setup(state_setup_definition: StateSetupDefinition) -> TestSetup:
    state_mock_setup = state_setup_definition.create_mock_setup()
    mock_scope_query_handler_context: MockScopeQueryHandlerContext = create_autospec(
        ScopeQueryHandlerContext
    )
    mock_execution_input: MockSQLStageGraphExecutionInput = create_autospec(
        SQLStageGraphExecutionInput
    )
    mock_state_factory: MockSQLStageGraphExecutionQueryHandlerStateFactory = Mock()
    mock_state_factory.return_value = state_mock_setup.state
    execution_query_handler = SQLStageGraphExecutionQueryHandler(
        parameter=mock_execution_input,
        query_handler_context=mock_scope_query_handler_context,
        query_handler_state_factory=mock_state_factory,
    )
    return TestSetup(
        execution_query_handler=execution_query_handler,
        mock_state_factory=mock_state_factory,
        mock_execution_input=mock_execution_input,
        mock_scope_query_handler_context=mock_scope_query_handler_context,
        state_mock_setup=state_mock_setup,
    )


def create_test_setup_with_two_query_handler_returning_continue_finish() -> (
    TestSetup
):
    state_setup_definition = StateSetupDefinition(
        query_handler_setup_definitions=[
            SQLStageQueryHandlerSetupDefinition(
                result_prototypes=[
                    Continue(query_list=None, input_query=None),
                    Finish(result=None),
                ]
            ),
            SQLStageQueryHandlerSetupDefinition(
                result_prototypes=[
                    Continue(query_list=None, input_query=None),
                    Finish(result=None),
                ]
            ),
        ]
    )
    test_setup = create_test_setup(state_setup_definition)
    test_setup.reset_mock()
    return test_setup


def test_init():
    """
    This test runs the constructor of SQLStageGraphExecutionQueryHandler and
    expects a call to the state factory with the execution input and the
    scope_query_handler_context.
    """

    def arrange() -> StateSetupDefinition:
        state_setup_definition = StateSetupDefinition(
            query_handler_setup_definitions=[
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[Finish(result=None)]
                )
            ]
        )
        return state_setup_definition

    def act(state_setup_definition: StateSetupDefinition) -> TestSetup:
        test_setup = create_test_setup(state_setup_definition)
        return test_setup

    state_setup_definiton = arrange()
    test_setup = act(state_setup_definiton)

    test_setup.mock_state_factory.assert_called_once_with(
        test_setup.mock_execution_input, test_setup.mock_scope_query_handler_context
    )
    test_setup.state_mock_setup.state.assert_not_called()
    test_setup.state_mock_setup.query_handler_mock_setups[
        0
    ].query_handler.assert_not_called()


def test_start_single_query_handler_returning_finish():
    """
    This test calls start on a newly created SQLStageGraphExecutionQueryHandler
    which was initialized with a state that has a query_handler which returns Finish.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the result of the query_handler
        - that start is called on the query_handler
        - that the result is equal to the result of query_handler.start
    """

    def arrange() -> TestSetup:
        state_setup_definition = StateSetupDefinition(
            query_handler_setup_definitions=[
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[Finish(result=None)]
                )
            ]
        )
        test_setup = create_test_setup(state_setup_definition)
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    query_handler_mock_setup = (
        test_setup.state_mock_setup.query_handler_mock_setups[0]
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setup.results[0]),
        ]
    )
    query_handler_mock_setup.query_handler.assert_has_calls([call.start()])
    mock_cast(
        query_handler_mock_setup.query_handler.handle_query_result
    ).assert_not_called()
    assert result == query_handler_mock_setup.results[0]


def test_start_single_query_handler_returning_continue():
    """
    This test calls start on a newly created SQLStageGraphExecutionQueryHandler
    which was initialized with a state that has a query_handler which returns Continue.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the result of the query_handler
        - that start is called on the query_handler
        - that the result is equal to the result of query_handler.start
    """

    def arrange() -> TestSetup:
        state_setup_definition = StateSetupDefinition(
            query_handler_setup_definitions=[
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[
                        Continue(query_list=None, input_query=None),
                    ]
                )
            ]
        )
        test_setup = create_test_setup(state_setup_definition)
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    query_handler_mock_setup = (
        test_setup.state_mock_setup.query_handler_mock_setups[0]
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setup.results[0]),
        ]
    )
    mock_cast(
        query_handler_mock_setup.query_handler.start
    ).assert_called_once()
    mock_cast(
        query_handler_mock_setup.query_handler.handle_query_result
    ).assert_not_called()
    assert result == query_handler_mock_setup.results[0]


def test_handle_query_result_single_query_handler_returning_continue_finish():
    """
    This test calls handle_query_result on a SQLStageGraphExecutionQueryHandler
    which was initialized with a state that has a query_handler which returns first Finish
    and then Continue.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the result of the query_handler
        - that handle_query_result is called on the query_handler
        - that the result is equal to the result of query_handler.handle_query_result
    """

    def arrange() -> Tuple[TestSetup, MockQueryResult]:
        state_setup_definition = StateSetupDefinition(
            query_handler_setup_definitions=[
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[
                        Continue(query_list=None, input_query=None),
                        Finish(result=None),
                    ]
                )
            ]
        )
        test_setup = create_test_setup(state_setup_definition)
        test_setup.execution_query_handler.start()
        test_setup.reset_mock()
        query_result: MockQueryResult = create_autospec(QueryResult)
        return test_setup, query_result

    def act(
        test_setup: TestSetup, query_Result: MockQueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.handle_query_result(query_result)
        return result

    test_setup, query_result = arrange()
    result = act(test_setup, query_result)

    query_handler_mock_setup = (
        test_setup.state_mock_setup.query_handler_mock_setups[0]
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setup.results[1]),
        ]
    )
    mock_cast(
        query_handler_mock_setup.query_handler.handle_query_result
    ).assert_called_once_with(query_result)
    mock_cast(
        query_handler_mock_setup.query_handler.start
    ).assert_not_called()
    assert result == query_handler_mock_setup.results[1]


def test_start_two_query_handler_returning_finish():
    """
    This test calls start on a newly created SQLStageGraphExecutionQueryHandler
    which was initialized with a state that has two query_handler which return Finish
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the result of the first query_handler
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the result of the second query_handler
        - that start is called on the first query_handler
        - that start is called on the second query_handler
        - that the result is equal to the result of the second query_handler.start
    """

    def arrange() -> TestSetup:
        state_setup_definition = StateSetupDefinition(
            query_handler_setup_definitions=[
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[Finish(result=None)]
                ),
                SQLStageQueryHandlerSetupDefinition(
                    result_prototypes=[Finish(result=None)]
                ),
            ]
        )
        test_setup = create_test_setup(state_setup_definition)
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    query_handler_mock_setups = (
        test_setup.state_mock_setup.query_handler_mock_setups
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[0].results[0]),
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[1].results[0]),
        ]
    )
    mock_cast(
        query_handler_mock_setups[0].query_handler.start
    ).assert_called_once()
    mock_cast(
        query_handler_mock_setups[1].query_handler.start
    ).assert_called_once()
    assert result == query_handler_mock_setups[1].results[0]


def test_start_two_query_handler_returning_continue_finish_part1():
    """
    This test calls start on a newly created SQLStageGraphExecutionQueryHandler
    which was initialized with a state that has two query_handler which returns first Continue
    and then Finish.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the first result of the first query_handler
        - that start is called on the first query_handler
        - that the result is equal to the result of the second query_handler.start
    """

    def arrange() -> TestSetup:
        test_setup = (
            create_test_setup_with_two_query_handler_returning_continue_finish()
        )
        test_setup.reset_mock()
        return test_setup

    def act(test_setup: TestSetup) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.start()
        return result

    test_setup = arrange()
    result = act(test_setup)

    query_handler_mock_setups = (
        test_setup.state_mock_setup.query_handler_mock_setups
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[0].results[0]),
        ]
    )
    mock_cast(
        query_handler_mock_setups[0].query_handler.start
    ).assert_called_once()
    mock_cast(
        query_handler_mock_setups[1].query_handler.start
    ).assert_not_called()
    mock_cast(
        query_handler_mock_setups[1].query_handler.handle_query_result
    ).assert_not_called()
    assert result == query_handler_mock_setups[0].results[0]


def test_handle_query_result_two_query_handler_returning_continue_finish_part2():
    """
    This test uses test_start_two_query_handler_returning_continue_finish_part1 as setup and
    calls again handle_query_result.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the second result of the first query_handler
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the first result of the second query_handler
        - that handle_query_result is called on the first query_handler
        - that start is called on the second query_handler
        - that the result is equal to the result of the second query_handler.start
    """

    def arrange() -> Tuple[TestSetup, QueryResult]:
        test_setup = (
            create_test_setup_with_two_query_handler_returning_continue_finish()
        )
        test_setup.execution_query_handler.start()
        test_setup.reset_mock()
        query_result: MockQueryResult = create_autospec(QueryResult)
        return test_setup, query_result

    def act(
        test_setup: TestSetup, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.handle_query_result(query_result)
        return result

    test_setup, query_result = arrange()
    result = act(test_setup, query_result)

    query_handler_mock_setups = (
        test_setup.state_mock_setup.query_handler_mock_setups
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[0].results[1]),
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[1].results[0]),
        ]
    )
    mock_cast(
        query_handler_mock_setups[0].query_handler.start
    ).assert_not_called()
    mock_cast(
        query_handler_mock_setups[0].query_handler.handle_query_result
    ).assert_called_once()
    mock_cast(
        query_handler_mock_setups[1].query_handler.start
    ).assert_called_once()
    mock_cast(
        query_handler_mock_setups[1].query_handler.handle_query_result
    ).assert_not_called()
    assert result == query_handler_mock_setups[1].results[0]


def test_handle_query_result_two_query_handler_returning_continue_finish_part3():
    """
    This test uses test_start_two_query_handler_returning_continue_finish_part2 as setup and
    calls again handle_query_result.
    It expects:
        - that get_current_query_handler is called on the state
        - that handle_result is called on the state with the second result of the second query_handler
        - that handle_query_result is called on the second query_handler
        - that the result is equal to the result of the second query_handler.handle_query_result
    """

    def arrange() -> Tuple[TestSetup, QueryResult]:
        test_setup = (
            create_test_setup_with_two_query_handler_returning_continue_finish()
        )
        query_result1: MockQueryResult = create_autospec(QueryResult)
        test_setup.execution_query_handler.start()
        test_setup.execution_query_handler.handle_query_result(query_result1)
        test_setup.reset_mock()
        query_result2: MockQueryResult = create_autospec(QueryResult)
        return test_setup, query_result2

    def act(
        test_setup: TestSetup, query_result: QueryResult
    ) -> Union[Continue, Finish[SQLStageInputOutput]]:
        result = test_setup.execution_query_handler.handle_query_result(query_result)
        return result

    test_setup, query_result = arrange()
    result = act(test_setup, query_result)

    query_handler_mock_setups = (
        test_setup.state_mock_setup.query_handler_mock_setups
    )
    test_setup.state_mock_setup.state.assert_has_calls(
        [
            call.get_current_query_handler(),
            call.handle_result(query_handler_mock_setups[1].results[1]),
        ]
    )
    mock_cast(
        query_handler_mock_setups[0].query_handler.start
    ).assert_not_called()
    mock_cast(
        query_handler_mock_setups[0].query_handler.handle_query_result
    ).assert_not_called()
    mock_cast(
        query_handler_mock_setups[1].query_handler.start
    ).assert_not_called()
    mock_cast(
        query_handler_mock_setups[1].query_handler.handle_query_result
    ).assert_called_once_with(query_result)
    assert result == query_handler_mock_setups[1].results[1]
