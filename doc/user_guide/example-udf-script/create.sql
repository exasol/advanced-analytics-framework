--/
CREATE OR REPLACE PYTHON3_AAF SET SCRIPT "EXAMPLE_SCHEMA"."EXAMPLE_QUERY_HANDLER_UDF"(...)
EMITS (outputs VARCHAR(2000000)) AS

from typing import Union
from exasol.analytics.query_handler.udf.interface import UDFQueryHandler
from exasol.analytics.utils.dynamic_modules import create_module
from exasol.analytics.query_handler.context.query_handler_context import QueryHandlerContext
from exasol.analytics.query_handler.result.interface import QueryResult
from exasol.analytics.query_handler.result.impl import Result, Continue, Finish
from exasol.analytics.query_handler.query.select import SelectQuery, SelectQueryWithColumnDefinition
from exasol.analytics.query_handler.context.proxy.bucketfs_location_proxy import     BucketFSLocationProxy
from exasol.analytics.schema import (
    Column,
    ColumnType,
    ColumnName,
)
from datetime import datetime
from exasol.bucketfs import as_string


example_module = create_module("example_module")

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
            table_name = self.db_table_proxy.fully_qualified
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


example_module.add_to_module(ExampleQueryHandler)

class ExampleQueryHandlerFactory:
    def create(self, parameter: str, query_handler_context: QueryHandlerContext):
        return example_module.ExampleQueryHandler(parameter, query_handler_context)

example_module.add_to_module(ExampleQueryHandlerFactory)

from exasol.analytics.query_handler.udf.runner.udf \
    import QueryHandlerRunnerUDF

udf = QueryHandlerRunnerUDF(exa)

def run(ctx):
    return udf.run(ctx)

/
