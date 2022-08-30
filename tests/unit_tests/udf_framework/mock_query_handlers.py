from typing import Dict, Any, Union

from exasol_data_science_utils_python.schema.column import \
    Column
from exasol_data_science_utils_python.schema.column_name import \
    ColumnName
from exasol_data_science_utils_python.schema.column_type import \
    ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition, \
    SelectQuery
from exasol_advanced_analytics_framework.query_handler.query_handler \
    import QueryHandler
from exasol_advanced_analytics_framework.query_handler.result \
    import Result, Finish, Continue
from exasol_advanced_analytics_framework.query_result.query_result \
    import QueryResult
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler

FINAL_RESULT = {"a": "1"}
QUERY_LIST = [SelectQuery("SELECT 1 FROM DUAL"), SelectQuery("SELECT 2 FROM DUAL")]


class MockQueryHandlerWithOneIteration(UDFQueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass


class MockQueryHandlerWithTwoIterations(UDFQueryHandler):
    def __init__(self,
                 parameter: Dict[str, Any],
                 query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        return_query = "SELECT a, table1.b, c FROM table1, table2 " \
                       "WHERE table1.b=table2.b"
        return_query_columns = [
            Column(ColumnName("a"), ColumnType("INTEGER")),
            Column(ColumnName("b"), ColumnType("INTEGER"))]
        query_handler_return_query = SelectQueryWithColumnDefinition(
            query_string=return_query,
            output_columns=return_query_columns)
        query_handler_result = Continue(
            query_list=QUERY_LIST,
            input_query=query_handler_return_query)
        return query_handler_result

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        query_handler_result = Finish(
            result=FINAL_RESULT)
        return query_handler_result


class QueryHandlerTestWithOneIterationAndTempTable(QueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        self._query_handler_context.get_temporary_table()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass


class MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext(UDFQueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.child = None

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass


class MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObject(UDFQueryHandler):

    def __init__(self, parameter: Dict[str, Any], query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.proxy = None
        self.child = None

    def start(self) -> Union[Continue, Finish[Dict[str, Any]]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        self.proxy = self.child.get_temporary_table()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[Dict[str, Any]]]:
        pass
