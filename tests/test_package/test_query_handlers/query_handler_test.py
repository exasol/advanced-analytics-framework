from typing import Dict, Any, Union

from exasol_data_science_utils_python.schema.column import \
    Column
from exasol_data_science_utils_python.schema.column_name \
    import ColumnName
from exasol_data_science_utils_python.schema.column_type \
    import ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQuery, \
    SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.query_handler \
    import QueryHandler, ResultType
from exasol_advanced_analytics_framework.query_handler.result \
    import Finish, Continue
from exasol_advanced_analytics_framework.query_result.query_result \
    import QueryResult

FINAL_RESULT = {"result": 1}
QUERY_LIST = [SelectQuery("SELECT 1 FROM DUAL"), SelectQuery("SELECT 2 FROM DUAL")]


class QueryHandlerTestWithOneIteration(QueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[ResultType]]:
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass


class QueryHandlerTestWithTwoIteration(QueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        return_query = "SELECT 1 AS COL1, 2 AS COL2 FROM DUAL"
        return_query_columns = [
            Column(ColumnName("COL1"), ColumnType("INTEGER")),
            Column(ColumnName("COL2"), ColumnType("INTEGER"))]
        query_handler_return_query = SelectQueryWithColumnDefinition(
            query_string=return_query,
            output_columns=return_query_columns)
        query_handler_result = Continue(
            query_list=QUERY_LIST,
            input_query=query_handler_return_query)
        return query_handler_result

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        return Finish(result=FINAL_RESULT)


class QueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext(QueryHandler):
    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.child = None

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass


class QueryHandlerWithOneIterationWithNotReleasedTemporaryObject(QueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.proxy = None
        self.child = None

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        self.proxy = self.child.get_temporary_table_name()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass
