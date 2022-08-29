from pathlib import PurePosixPath
from typing import Union

import pytest
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import LocalFSMockBucketFSLocation
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from exasol_data_science_utils_python.udf_utils.testing.mock_result_set import MockResultSet
from exasol_data_science_utils_python.udf_utils.testing.mock_sql_executor import MockSQLExecutor

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition, \
    SelectQuery
from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.testing.mock_query_handler_runner import MockQueryHandlerRunner


@pytest.fixture()
def temporary_schema_name():
    temporary_schema_name = "temp_schema_name"
    return temporary_schema_name


@pytest.fixture()
def top_level_query_handler_context(tmp_path, temporary_schema_name):
    top_level_query_handler_context = TopLevelQueryHandlerContext(
        temporary_bucketfs_location=LocalFSMockBucketFSLocation(base_path=PurePosixPath(tmp_path) / "bucketfs"),
        temporary_db_object_name_prefix="temp_db_object",
        temporary_schema_name=temporary_schema_name,
    )
    return top_level_query_handler_context


class TestInput:
    pass


class TestOutput:
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


def test_start_finish(top_level_query_handler_context):
    sql_executor = MockSQLExecutor(result_sets=[])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
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
        self._query_handler_context.get_temporary_table()
        return Finish[TestOutput](TestOutput(self._parameter))

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_start_finish_cleanup_queries(temporary_schema_name, top_level_query_handler_context):
    sql_executor = MockSQLExecutor(result_sets=[MockResultSet()])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=StartFinishCleanupQueriesTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input and \
           sql_executor.queries == [f"""DROP TABLE IF EXISTS "{temporary_schema_name}"."temp_db_object_1";"""]


class StartErrorCleanupQueriesTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_temporary_table()
        raise Exception("Start failed")

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        pass


def test_start_error_cleanup_queries(temporary_schema_name, top_level_query_handler_context):
    sql_executor = MockSQLExecutor(result_sets=[MockResultSet()])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=StartErrorCleanupQueriesTestQueryHandler
    )
    with pytest.raises(Exception, match="Execution of query handler .* failed."):
        test_output = query_handler_runner.run()
    assert sql_executor.queries == [f"""DROP TABLE IF EXISTS "{temporary_schema_name}"."temp_db_object_1";"""]


class ContinueFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if query_result.a != 1:
            raise AssertionError(f"query_result.a != 1, got {query_result.a}")
        return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_finish(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                        ColumnType(name="DECIMAL",
                                                                                   precision=1,
                                                                                   scale=0))])
    drop_input_query_view_result_set = MockResultSet()
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set,
            drop_input_query_view_result_set
        ])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=ContinueFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input and \
           sql_executor.queries == [
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_2_1" AS SELECT 1 as "a";""",
               f"""SELECT "a" FROM "{temporary_schema_name}"."temp_db_object_2_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_2_1";""",
           ]


class ContinueWrongColumnsTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if query_result.a != 1:
            raise AssertionError(f"query_result.a != 1, got {query_result.a}")
        return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_wrong_columns(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("b"),
                                                                        ColumnType(name="DECIMAL",
                                                                                   precision=1,
                                                                                   scale=0))])
    drop_input_query_view_result_set = MockResultSet()
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set,
            drop_input_query_view_result_set
        ])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
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
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        query_list = [SelectQuery(query_string="SELECT 1")]
        return Continue(query_list=query_list, input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if query_result.a != 1:
            raise AssertionError(f"query_result.a != 1, got {query_result.a}")
        return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_query_list(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                        ColumnType(name="DECIMAL",
                                                                                   precision=1,
                                                                                   scale=0))])
    drop_input_query_view_result_set = MockResultSet()
    query_list_result_set = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                       ColumnType(name="DECIMAL",
                                                                                  precision=1,
                                                                                  scale=0))])
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set,
            query_list_result_set,
            drop_input_query_view_result_set
        ])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=ContinueQueryListTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input and \
           sql_executor.queries == [
               f"""SELECT 1""",
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_2_1" AS SELECT 1 as "a";""",
               f"""SELECT "a" FROM "{temporary_schema_name}"."temp_db_object_2_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_2_1";""",
           ]


class ContinueErrorCleanupQueriesTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        self._query_handler_context.get_temporary_table()
        raise Exception("Start failed")


def test_continue_error_cleanup_queries(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                        ColumnType(name="DECIMAL",
                                                                                   precision=1,
                                                                                   scale=0))])
    drop_table_result_set = MockResultSet()
    drop_input_query_view_result_set = MockResultSet()
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set,
            drop_table_result_set,
            drop_input_query_view_result_set
        ])
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=ContinueErrorCleanupQueriesTestQueryHandler
    )
    with pytest.raises(Exception, match="Execution of query handler .* failed."):
        test_output = query_handler_runner.run()
    assert sql_executor.queries == [
        f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_2_1" AS SELECT 1 as "a";""",
        f"""SELECT "a" FROM "{temporary_schema_name}"."temp_db_object_2_1";""",
        f"""DROP TABLE IF EXISTS "{temporary_schema_name}"."temp_db_object_3";""",
        f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_2_1";""",
    ]


class ContinueContinueFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self._iter = 0

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if self._iter == 0:
            self._iter += 1
            column_name = ColumnName("b")
            input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                          [Column(column_name, ColumnType("INTEGER"))])
            return Continue(query_list=[], input_query=input_query)
        else:
            return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_continue_finish(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set1 = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                         ColumnType(name="DECIMAL",
                                                                                    precision=1,
                                                                                    scale=0))])
    input_query_result_set2 = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("b"),
                                                                         ColumnType(name="DECIMAL",
                                                                                    precision=1,
                                                                                    scale=0))])
    drop_input_query_view_result_set = MockResultSet()
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set1,
            drop_input_query_view_result_set,
            input_query_create_view_result_set,
            input_query_result_set2,
            drop_input_query_view_result_set,
        ])
    temporary_schema_name = "temp_schema_name"
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=ContinueContinueFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input and \
           sql_executor.queries == [
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_2_1" AS SELECT 1 as "a";""",
               f"""SELECT "a" FROM "{temporary_schema_name}"."temp_db_object_2_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_2_1";""",
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_4_1" AS SELECT 1 as "b";""",
               f"""SELECT "b" FROM "{temporary_schema_name}"."temp_db_object_4_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_4_1";""",
           ]


class ContinueContinueCleanupFinishTestQueryHandler(QueryHandler[TestInput, TestOutput]):
    def __init__(self, parameter: TestInput, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter
        self._iter = 0

    def start(self) -> Union[Continue, Finish[TestOutput]]:
        self._child_query_handler_conntext = self._query_handler_context.get_child_query_handler_context()
        self._table = self._child_query_handler_conntext.get_temporary_table()
        column_name = ColumnName("a")
        input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                      [Column(column_name, ColumnType("INTEGER"))])
        return Continue(query_list=[], input_query=input_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[TestOutput]]:
        if self._iter == 0:
            self._child_query_handler_conntext.release()
            self._iter += 1
            column_name = ColumnName("b")
            input_query = SelectQueryWithColumnDefinition(f"""SELECT 1 as {column_name.quoted_name()}""",
                                                          [Column(column_name, ColumnType("INTEGER"))])
            return Continue(query_list=[], input_query=input_query)
        else:
            return Finish[TestOutput](TestOutput(self._parameter))


def test_continue_cleanup_continue_finish(temporary_schema_name, top_level_query_handler_context):
    input_query_create_view_result_set = MockResultSet()
    input_query_result_set1 = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("a"),
                                                                         ColumnType(name="DECIMAL",
                                                                                    precision=1,
                                                                                    scale=0))])
    input_query_result_set2 = MockResultSet(rows=[(1,)], columns=[Column(ColumnName("b"),
                                                                         ColumnType(name="DECIMAL",
                                                                                    precision=1,
                                                                                    scale=0))])
    drop_input_query_view_result_set = MockResultSet()
    drop_table_result_set = MockResultSet()
    sql_executor = MockSQLExecutor(
        result_sets=[
            input_query_create_view_result_set,
            input_query_result_set1,
            drop_input_query_view_result_set,
            drop_table_result_set,
            input_query_create_view_result_set,
            input_query_result_set2,
            drop_input_query_view_result_set,
        ])
    temporary_schema_name = "temp_schema_name"
    test_input = TestInput()
    query_handler_runner = MockQueryHandlerRunner[TestInput, TestOutput](
        sql_executor=sql_executor,
        top_level_query_handler_context=top_level_query_handler_context,
        parameter=test_input,
        query_handler_factory=ContinueContinueCleanupFinishTestQueryHandler
    )
    test_output = query_handler_runner.run()
    assert test_output.test_input == test_input and \
           sql_executor.queries == [
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_4_1" AS SELECT 1 as "a";""",
               f"""SELECT "a" FROM "{temporary_schema_name}"."temp_db_object_4_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_4_1";""",
               f"""DROP TABLE IF EXISTS "{temporary_schema_name}"."temp_db_object_2_1";""",
               f"""CREATE VIEW "{temporary_schema_name}"."temp_db_object_6_1" AS SELECT 1 as "b";""",
               f"""SELECT "b" FROM "{temporary_schema_name}"."temp_db_object_6_1";""",
               f"""DROP VIEW IF EXISTS "{temporary_schema_name}"."temp_db_object_6_1";""",
           ]
