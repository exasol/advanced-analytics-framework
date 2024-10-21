from typing import Union
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler
from exasol_advanced_analytics_framework.udf_framework.dynamic_modules import create_module
from exasol_advanced_analytics_framework.query_handler.context.query_handler_context import QueryHandlerContext
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.query_handler.result import Result, Continue, Finish
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQuery, SelectQueryWithColumnDefinition
from exasol_advanced_analytics_framework.query_handler.context.proxy.bucketfs_location_proxy import \
    BucketFSLocationProxy
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from datetime import datetime
from exasol.bucketfs import as_string


xyz = create_module("xyz")

class ExampleQueryHandler(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: QueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.parameter = parameter
        self.query_handler_context = query_handler_context
        self.bfs_proxy = None
        self.db_table_proxy = None

    def _bfs_file(self, proxy: BucketFSLocationProxy):
        return proxy.bucketfs_location() / "temp_file.txt"

    def start(self) -> Union[Continue, Finish[str]]:
        def sample_content(key: str) -> str:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return f"{timestamp} {key} {self.parameter}"

        def table_query_string(statement: str, **kwargs):
            table_name = self.db_table_proxy._db_object_name.fully_qualified
            return statement.format(table_name=table_name, **kwargs)

        def table_query(statement: str, **kwargs):
            return SelectQuery(table_query_string(statement, **kwargs))

        self.bfs_proxy = self.query_handler_context.get_temporary_bucketfs_location()
        self._bfs_file(self.bfs_proxy).write(sample_content("bucketfs"))
        self.db_table_proxy = self.query_handler_context.get_temporary_table_name()
        query_list = [
            table_query('CREATE TABLE {table_name} ("c1" VARCHAR(100), "c2" INTEGER)'),
            table_query("INSERT INTO {table_name} VALUES ('{value}', 4)",
                        value=sample_content("table-insert")),
        ]
        query_handler_return_query = SelectQueryWithColumnDefinition(
            query_string=table_query_string('SELECT "c1", "c2" from {table_name}'),
            output_columns=[
                Column(ColumnName("c1"), ColumnType("VARCHAR(100)")),
                Column(ColumnName("c2"), ColumnType("INTEGER")),
            ])
        return Continue(
            query_list=query_list,
            input_query=query_handler_return_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        c1 = query_result.c1
        c2 = query_result.c2
        bfs_content = as_string(self._bfs_file(self.bfs_proxy).read())
        return Finish(result=f"Final result: from query '{c1}', {c2} and bucketfs: '{bfs_content}'")


xyz.add_to_module(ExampleQueryHandler)

class ExampleQueryHandlerFactory:
    def create(self, parameter: str, query_handler_context: QueryHandlerContext):
        return xyz.ExampleQueryHandler(parameter, query_handler_context)

xyz.add_to_module(ExampleQueryHandlerFactory)

from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf \
    import QueryHandlerRunnerUDF

udf = QueryHandlerRunnerUDF(exa)

def run(ctx):
    return udf.run(ctx)
