import logging
import textwrap
from inspect import cleandoc
from typing import Callable, TypeVar, Generic, Tuple, Union, List

from exasol_data_science_utils_python.udf_utils.sql_executor import SQLExecutor

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext, ConnectionLookup
from exasol_advanced_analytics_framework.query_handler.query.query import Query
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.mock_query_result import MockQueryResult
from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_state import QueryHandlerRunnerState

LOGGER = logging.getLogger(__file__)

ResultType = TypeVar("ResultType")
ParameterType = TypeVar("ParameterType")


class MockQueryHandlerRunner(Generic[ParameterType, ResultType]):

    def __init__(self,
                 sql_executor: SQLExecutor,
                 top_level_query_handler_context: TopLevelQueryHandlerContext,
                 parameter: ParameterType,
                 query_handler_factory: Callable[
                     [ParameterType, ScopeQueryHandlerContext],
                     QueryHandler[ParameterType, ResultType]]):
        self._sql_executor = sql_executor
        query_handler = query_handler_factory(parameter, top_level_query_handler_context)
        self._state = QueryHandlerRunnerState(
            top_level_query_handler_context=top_level_query_handler_context,
            query_handler=query_handler,
            connection_lookup=None
        )

    def run(self) -> ResultType:
        try:
            result = self._state.query_handler.start()
            while isinstance(result, Continue):
                result = self.handle_continue(result)
            if isinstance(result, Finish):
                self.handle_finish()
                return result.result
            else:
                raise RuntimeError("Unknown Result")
        except Exception as e:
            try:
                self.handle_finish()
            except Exception as e1:
                LOGGER.exception("Catched exeception during cleanup after an exception.")
            raise RuntimeError(f"Execution of query handler {self._state.query_handler} failed.") from e

    def handle_continue(self, result: Continue) -> Union[Continue, Finish[ResultType]]:
        self.release_and_create_query_handler_context_of_input_query()
        self.cleanup_query_handler_context()
        self.execute_query(result.query_list)
        input_query_result = self.run_input_query(result)
        result = self._state.query_handler.handle_query_result(input_query_result)
        return result

    def run_input_query(self, result: Continue) -> MockQueryResult:
        input_query_view, input_query = self._wrap_return_query(result.input_query)
        self._sql_executor.execute(input_query_view)
        input_query_result_set = self._sql_executor.execute(input_query)
        if input_query_result_set.columns() != result.input_query.output_columns:
            raise RuntimeError(f"Specified columns {result.input_query.output_columns} of the input query "
                               f"are not equal to the actual received columns {input_query_result_set.columns()}")
        input_query_result_table = input_query_result_set.fetchall()
        input_query_result = MockQueryResult(data=input_query_result_table,
                                             columns=result.input_query.output_columns)
        return input_query_result

    def handle_finish(self):
        if self._state.input_query_query_handler_context is not None:
            self._state.input_query_query_handler_context.release()
        self._state.top_level_query_handler_context.release()
        self.cleanup_query_handler_context()

    def cleanup_query_handler_context(self):
        cleanup_query_list = \
            self._state.top_level_query_handler_context.cleanup_released_object_proxies()
        self.execute_query(cleanup_query_list)

    def execute_query(self, queries: List[Query]):
        for query in queries:
            self._sql_executor.execute(query.query_string)

    def release_and_create_query_handler_context_of_input_query(self):
        if self._state.input_query_query_handler_context is not None:
            self._state.input_query_query_handler_context.release()
        self._state.input_query_query_handler_context = \
            self._state.top_level_query_handler_context.get_child_query_handler_context()

    def _wrap_return_query(self, input_query: SelectQueryWithColumnDefinition) -> Tuple[str, str]:
        temporary_view_name = self._state.input_query_query_handler_context.get_temporary_view_name()
        input_query_create_view_string = cleandoc(
            f"""
CREATE VIEW {temporary_view_name.fully_qualified} AS 
{input_query.query_string};
""")
        full_qualified_columns = [col.name.fully_qualified
                                  for col in input_query.output_columns]
        columns_str = ",\n".join(full_qualified_columns)
        input_query_string = cleandoc(
            f"""
SELECT 
{textwrap.indent(columns_str, " " * 4)}
FROM {temporary_view_name.fully_qualified};
""")
        return input_query_create_view_string, input_query_string
