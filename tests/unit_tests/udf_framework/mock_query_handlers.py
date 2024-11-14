from typing import Dict, Any, Union

from exasol.analytics.schema.column import \
    Column
from exasol.analytics.schema.column_name import \
    ColumnName
from exasol.analytics.schema.column_type import \
    ColumnType

from exasol.analytics.query_handler.context.scope import     ScopeQueryHandlerContext
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition,     SelectQuery
from exasol.analytics.query_handler.result     import Finish, Continue
from exasol.analytics.query_handler.query.result.interface     import QueryResult
from exasol.analytics.query_handler.udf.interface import UDFQueryHandler
from exasol.analytics.query_handler.udf.interface import UDFQueryHandlerFactory

TEST_CONNECTION = "TEST_CONNECTION"

TEST_INPUT = "<<TEST_INPUT>>"
FINAL_RESULT = '<<FINAL_RESULT>>'
QUERY_LIST = [SelectQuery("SELECT 1 FROM DUAL"), SelectQuery("SELECT 2 FROM DUAL")]


class MockQueryHandlerWithOneIteration(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        if not isinstance(parameter, str):
            raise AssertionError(f"Expected parameter={parameter} to be a string.")
        if parameter != TEST_INPUT:
            raise AssertionError(f"Expected parameter={parameter} to be '{TEST_INPUT}'.")

    def start(self) -> Union[Continue, Finish[str]]:
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass


class MockQueryHandlerWithOneIterationFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return MockQueryHandlerWithOneIteration(parameter, query_handler_context)


class MockQueryHandlerWithTwoIterations(UDFQueryHandler):
    def __init__(self,
                 parameter: str,
                 query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[str]]:
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
        a = query_result.a
        if a != 1:
            raise AssertionError(f"Expected query_result.a={a} to be 1.")
        b = query_result.b
        if b != 2:
            raise AssertionError(f"Expected query_result.b={b} to be 2.")
        has_next = query_result.next()
        if has_next:
            raise AssertionError(f"No next row expected")
        query_handler_result = Finish(result=FINAL_RESULT)
        return query_handler_result


class MockQueryHandlerWithTwoIterationsFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return MockQueryHandlerWithTwoIterations(parameter, query_handler_context)


class QueryHandlerTestWithOneIterationAndTempTable(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[str]]:
        self._query_handler_context.get_temporary_table_name()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass


class QueryHandlerTestWithOneIterationAndTempTableFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return QueryHandlerTestWithOneIterationAndTempTable(parameter, query_handler_context)


class MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.child = None

    def start(self) -> Union[Continue, Finish[str]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass


class MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContextFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return MockQueryHandlerWithOneIterationWithNotReleasedChildQueryHandlerContext(parameter, query_handler_context)


class MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObject(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.proxy = None
        self.child = None

    def start(self) -> Union[Continue, Finish[str]]:
        self.child = self._query_handler_context.get_child_query_handler_context()
        self.proxy = self.child.get_temporary_table_name()
        return Finish(result=FINAL_RESULT)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass


class MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObjectFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return MockQueryHandlerWithOneIterationWithNotReleasedTemporaryObject(parameter, query_handler_context)


class MockQueryHandlerUsingConnection(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[str]]:
        connection = self._query_handler_context.get_connection(TEST_CONNECTION)
        return Finish(
            f"{connection.name},{connection.address},{connection.user},{connection.password}")

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass


class MockQueryHandlerUsingConnectionFactory(UDFQueryHandlerFactory):

    def create(self, parameter: str, query_handler_context: ScopeQueryHandlerContext) -> UDFQueryHandler:
        return MockQueryHandlerUsingConnection(parameter, query_handler_context)
