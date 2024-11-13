import re
from pathlib import PurePosixPath
from typing import List, Union
from inspect import cleandoc

import pytest
from exasol.analytics.schema import (
    Column,
    ColumnType,
    ColumnName,
)
from exasol.analytics.sql_executor.testing.mock_result_set import MockResultSet
from exasol.analytics.sql_executor.testing.mock_sql_executor import MockSQLExecutor, ExpectedQuery

from exasol.analytics.query_handler.context.scope import     ScopeQueryHandlerContext
from exasol.analytics.query_handler.context.top_level_query_handler_context import     TopLevelQueryHandlerContext
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition,     SelectQuery
from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol.analytics.query_handler.result.impl import Continue, Finish
from exasol.analytics.query_handler.result.interface import QueryResult
from exasol.analytics.query_handler.python_query_handler_runner import PythonQueryHandlerRunner

EXPECTED_EXCEPTION = "ExpectedException"


def expect_query(template: str, result_set = MockResultSet()):
    return [template, result_set]


def create_sql_executor_1(schema: str, *args):
    return MockSQLExecutor([
        ExpectedQuery(
            cleandoc(template.format(schema=schema)),
            result_set or MockResultSet()
        ) for template, result_set in args
    ])


def create_sql_executor(schema: str, prefix: str, *args):
    return MockSQLExecutor([
        ExpectedQuery(
            cleandoc(template.format(schema=schema, prefix=prefix)),
            result_set or MockResultSet()
        ) for template, result_set in args
    ])


@pytest.fixture()
def prefix(tmp_db_obj_prefix):
    return tmp_db_obj_prefix


@pytest.fixture
def context_mock(top_level_query_handler_context_mock) -> TopLevelQueryHandlerContext:
    return top_level_query_handler_context_mock


class TestInput:
    pass


class TestOutput:
    __test__ = False
    def __init__(self, test_input: TestInput):
        self.test_input = test_input


class StartFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        return Finish[TestOutput](TestOutput(self._parameter))

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_start_finish(context_mock):
    """
    This tests runs a query handler which returns a Finish result from the start method.
    We expect no queries to be executed and result of the Finish object returned.
    """
    sql_executor = MockSQLExecutor()
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=StartFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class StartFinishCleanupQueriesTestQueryHandler(QueryHandler[TestInput, TestOutput]):

    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_temporary_table_name()
        return Finish[TestOutput](TestOutput(self._parameter))

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_start_finish_cleanup_queries(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which registers a temporary table in the start method
    and then directly returns a Finish result. We expect a cleanup query for the temporary
    table to be executed and result of the Finish object returned.
    """
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            'DROP TABLE IF EXISTS "{schema}"."{prefix}_1";'
        ))

    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=StartFinishCleanupQueriesTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class StartErrorCleanupQueriesTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_temporary_table_name()
        raise Exception("Start failed")

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_start_error_cleanup_queries(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which registers a temporary table in the start method
    and then directly raise an exception. We expect a cleanup query for the temporary
    table to be executed and the exception to be forwarded.
    """
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            'DROP TABLE IF EXISTS "{schema}"."{prefix}_1";'
        ))

    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=StartErrorCleanupQueriesTestQueryHandler
    )
    with pytest.raises(Exception, match="Execution of query handler .* failed.") as ex:
        test_output = query_handler_runner.run()
    assert ex.value.__cause__.args[0] == "Start failed"


class ContinueFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name}""",
                                                      [Column(ColumnName("a"),
                                                              ColumnType(name="DECIMAL",
                                                                         precision=1,
                                                                         scale=0))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if query_result.a != 1:
            raise AssertionError(f"query_result.a != 1, got {query_result.a}")
        return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_finish(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which returns Continue result from the start method
    and expect handle_query_result to be called. Further, it expects that
    handle_query_result can access the columns in the resultset which where defined
    in the input_query.
    """
    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_2_1" AS
            SELECT 1 as "a";
            """
        ),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_2_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_2_1";'
        ),
    )

    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class ContinueWrongColumnsTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {ColumnName("b").quoted_name}""",
                                                      [Column(ColumnName("a"), ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        raise AssertionError("handle_query_result shouldn't be called")


def test_continue_wrong_columns(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which returns Continue result with mismatching column definition
    between the input query and its column definition. We expect the query handler runner to raise
    an Exception.
    """

    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_2_1" AS
            SELECT 1 as "b";
            """
        ),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_2_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("b"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_2_1";'
        ),
    )
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueWrongColumnsTestQueryHandler
    )
    with pytest.raises(RuntimeError) as exception:
        test_output = query_handler_runner.run()
    assert "Specified columns" in exception.value.__cause__.args[0]


class ContinueQueryListTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name}""",
                                                      [Column(ColumnName("a"),
                                                              ColumnType(name="DECIMAL",
                                                                         precision=1,
                                                                         scale=0))])
        query_list = [SelectQuery(query_string="SELECT 1")]
        return Continue(query_list=query_list, input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_query_list(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which returns Continue result from the start method
    which contains a query list. We expect to be handle_query_result to be called and
    the queries in the query list to be executed.
    """

    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query("SELECT 1"),
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_2_1" AS
            SELECT 1 as "a";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_2_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_2_1";'
        ),
    )
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueQueryListTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class ContinueErrorCleanupQueriesTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name}""",
                                                      [Column(ColumnName("a"),
                                                              ColumnType(name="DECIMAL",
                                                                         precision=1,
                                                                         scale=0))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_temporary_table_name()
        raise Exception("Start failed")


def test_continue_error_cleanup_queries(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which registers a temporary table in the handle_query_result method
    and then directly raise an exception. We expect a cleanup query for the temporary
    table to be executed and the exception to be forwarded.
    """

    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_2_1" AS
            SELECT 1 as "a";
            """),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_2_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query('DROP TABLE IF EXISTS "{schema}"."{prefix}_3";'),
        expect_query('DROP VIEW IF EXISTS "{schema}"."{prefix}_2_1";'),
    )
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueErrorCleanupQueriesTestQueryHandler
    )
    with pytest.raises(Exception, match="Execution of query handler .* failed."):
        test_output = query_handler_runner.run()


class ContinueContinueFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):

    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self._iter = 0

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(
            f"""SELECT 1 as {column_name.quoted_name}""",
            [Column(ColumnName("a"),
                    ColumnType(name="DECIMAL",
                               precision=1,
                               scale=0))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if self._iter == 0:
            self._iter += 1
            column_name = ColumnName("b")
            input_query = SelectQueryWithColumnDefinition(
                f"""SELECT 1 as {column_name.quoted_name}""",
                [Column(ColumnName("b"),
                        ColumnType(name="DECIMAL",
                                   precision=1,
                                   scale=0))])
            return Continue(query_list=[], input_query=input_query)
        else:
            return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_continue_finish(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which returns Continue from the first call to handle_query_result method
    and the second time it returns Finish. We expect two input queries to be executed; one per Continue and
    in the end the result should be returned.
    """

    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_2_1" AS
            SELECT 1 as "a";
            """),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_2_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_2_1";'
        ),
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_4_1" AS
            SELECT 1 as "b";
            """),
        expect_query(
            """
            SELECT
                "b"
            FROM "{schema}"."{prefix}_4_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("b"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_4_1";'),
    )
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueContinueFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class ContinueContinueCleanupFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self._iter = 0

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._child_query_handler_conntext = self._query_handler_context.get_child_query_handler_context()
        self._table = self._child_query_handler_conntext.get_temporary_table_name()
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name}""",
                                                      [Column(ColumnName("a"),
                                                              ColumnType(name="DECIMAL",
                                                                         precision=1,
                                                                         scale=0))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if self._iter == 0:
            self._child_query_handler_conntext.release()
            self._iter += 1
            column_name = ColumnName("b")
            input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name}""",
                                                          [Column(ColumnName("b"),
                                                                  ColumnType(name="DECIMAL",
                                                                             precision=1,
                                                                             scale=0))])
            return Continue(query_list=[], input_query=input_query)
        else:
            return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_cleanup_continue_finish(aaf_pytest_db_schema, prefix, context_mock):
    """
    This tests runs a query handler which creates the temporary table of a child query context manager.
    Then it returns a Continue result, such that handle_query_result will be called. During the call to
    handle_query_result we release child query context manager and return a Continue result such that
    handle_query_result gets called again, which then returns a Finish result.
    We expect that the cleanup of the temporary happens between the first and second call to handle_query_result.
    """

    sql_executor = create_sql_executor(
        aaf_pytest_db_schema,
        prefix,
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_4_1" AS
            SELECT 1 as "a";
            """),
        expect_query(
            """
            SELECT
                "a"
            FROM "{schema}"."{prefix}_4_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("a"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_4_1";'
        ),
        expect_query(
            'DROP TABLE IF EXISTS "{schema}"."{prefix}_2_1";'
        ),
        expect_query(
            """
            CREATE OR REPLACE VIEW "{schema}"."{prefix}_6_1" AS
            SELECT 1 as "b";
            """),
        expect_query(
            """
            SELECT
                "b"
            FROM "{schema}"."{prefix}_6_1";
            """,
            MockResultSet(
                rows=[(1,)],
                columns=[Column(ColumnName("b"), ColumnType(
                    name="DECIMAL", precision=1, scale=0))])
        ),
        expect_query(
            'DROP VIEW IF EXISTS "{schema}"."{prefix}_6_1";'
        ),
    )
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=ContinueContinueCleanupFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input


class FailInCleanupAfterException(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self._iter = 0

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_child_query_handler_context()
        raise Exception(EXPECTED_EXCEPTION)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_fail_in_cleanup(aaf_pytest_db_schema, context_mock):
    sql_executor = MockSQLExecutor()
    test_input = TestInput()
    query_handler_runner = PythonQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=context_mock,
        parameter=test_input,
        query_handler_factory=FailInCleanupAfterException
    )

    with pytest.raises(RuntimeError, match="Execution of query handler .* failed.") as e:
        query_handler_runner.run()

    assert e.value.__cause__.args[0] == EXPECTED_EXCEPTION
