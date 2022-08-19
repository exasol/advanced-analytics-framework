from typing import Dict, Any

from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column
from exasol_data_science_utils_python.preprocessing.sql.schema.column_name import \
    ColumnName
from exasol_data_science_utils_python.preprocessing.sql.schema.column_type import \
    ColumnType

from exasol_advanced_analytics_framework.query_result.query_result \
    import QueryResult
from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query_handler \
    import QueryHandler
from exasol_advanced_analytics_framework.query_handler.context.query_handler_context \
    import QueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.result \
    import Result, Finished, \
    ReturnQuery, Continue

FINAL_RESULT = {"a": "1"}
QUERY_LIST = ["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"]


class MockQueryHandlerWithOneIteration(QueryHandler):
    def handle_event(self,
                     query_result: QueryResult,
                     query_handler_context: QueryHandlerContext) -> \
            Result:
        return Finished(final_result=FINAL_RESULT)


class MockQueryHandlerWithTwoIterations(QueryHandler):
    def __init__(self, parameters: Dict[str, Any]):
        super().__init__(parameters)
        self.iter = 0

    def handle_event(self,
                     query_result: QueryResult,
                     query_handler_context: QueryHandlerContext) -> \
            Result:

        if self.iter > 0:
            query_handler_result = Finished(
                final_result=FINAL_RESULT)
        else:
            return_query = "SELECT a, table1.b, c FROM table1, table2 " \
                           "WHERE table1.b=table2.b"
            return_query_columns = [
                Column(ColumnName("a"), ColumnType("INTEGER")),
                Column(ColumnName("b"), ColumnType("INTEGER"))]
            query_handler_return_query = ReturnQuery(
                query=return_query,
                query_columns=return_query_columns)
            query_handler_result = Continue(
                query_list=QUERY_LIST,
                return_query=query_handler_return_query)
        self.iter += 1
        return query_handler_result


class QueryHandlerTestWithOneIterationAndTempTable(QueryHandler):
    def handle_event(self,
                     query_result: QueryResult,
                     query_handler_context: QueryHandlerContext) \
            -> Result:
        query_handler_context.get_temporary_table()
        return Finished(final_result=FINAL_RESULT)


class MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext(QueryHandler):
    def handle_event(self,
                     query_result: QueryResult,
                     query_handler_context: ScopeQueryHandlerContext) -> \
            Result:
        self.child = query_handler_context.get_child_query_handler_context()
        return Finished(final_result=FINAL_RESULT)

class MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObject(QueryHandler):
    def handle_event(self,
                     query_result: QueryResult,
                     query_handler_context: ScopeQueryHandlerContext) -> \
            Result:
        self.child = query_handler_context.get_child_query_handler_context()
        self.proxy = self.child.get_temporary_table()
        return Finished(final_result=FINAL_RESULT)
