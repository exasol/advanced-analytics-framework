open schema "MY_SCHEMA";

ALTER SYSTEM SET SCRIPT_LANGUAGES='R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3 PYTHON3_AAF=localzmq+protobuf:///bfsdefault/default/temp/exasol_advanced_analytics_framework_container_release?lang=python#/buckets/bfsdefault/default/temp/exasol_advanced_analytics_framework_container_release/exaudf/exaudfclient_py3';

--/
CREATE OR REPLACE PYTHON3_AAF SET SCRIPT "MY_SCHEMA"."MY_QUERY_HANDLER_UDF"(...)
EMITS (outputs VARCHAR(2000000)) AS

from typing import Union
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler
from exasol_advanced_analytics_framework.query_handler.context.query_handler_context import QueryHandlerContext
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.query_handler.result import Result, Continue, Finish
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQuery, SelectQueryWithColumnDefinition
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from datetime import datetime
from io import BytesIO, TextIOWrapper


# proposal write parameter into temp table in schema ...
class CustomQueryHandler(UDFQueryHandler):
    def __init__(self, parameter: str, query_handler_context: QueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.parameter = parameter
        self.query_handler_context = query_handler_context
        self.bfs_proxy = query_handler_context.get_temporary_bucketfs_location()
        self.db_table_proxy = query_handler_context.get_temporary_table_name()

    @property
    def _bfs_temp_file(self):
        return self.bfs_proxy.bucketfs_location() / "temp_file.txt"

    def _sample_content(self, key: str) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{timestamp} {key} {self.parameter}"

    def _write_to_bucketfs(self):
        self._bfs_temp_file.write(self._sample_content("bucketfs"))

    def _read_from_bucketfs(self) -> str:
        buffer = BytesIO()
        for chunk in self._bfs_temp_file.read():
            buffer.write(chunk)
        return buffer.getvalue().decode("utf-8")

    def _write_to_db_table(self):
        self.db_table_proxy().insert(self._sample_content("table-insert"))

    def start(self) -> Union[Continue, Finish[str]]:
        self._write_to_bucketfs()
        # self._write_to_db_table()
        query_list = [
          SelectQuery("SELECT 1 FROM DUAL"),
          SelectQuery("SELECT 2 FROM DUAL")]
        query_handler_return_query = SelectQueryWithColumnDefinition(
            query_string="SELECT 5 AS 'return_column' FROM DUAL",
            output_columns=[
              Column(ColumnName("return_column"), ColumnType("INTEGER"))])
        return Continue(
            query_list=query_list,
            input_query=query_handler_return_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        return_value = query_result.return_column
        result = 2 ** return_value
        s = self._read_from_bucketfs()
        return Finish(result=f"Assertion of the final result: 32 == {result} content from bucketfs: '{s}'")

import builtins
builtins.CustomQueryHandler=CustomQueryHandler # required for pickle

class CustomQueryHandlerFactory:
      def create(self, parameter: str, query_handler_context: QueryHandlerContext):
          return builtins.CustomQueryHandler(parameter, query_handler_context)

builtins.CustomQueryHandlerFactory=CustomQueryHandlerFactory

from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf \
    import QueryHandlerRunnerUDF

udf = QueryHandlerRunnerUDF(exa)

def run(ctx):
    return udf.run(ctx)
/


EXECUTE SCRIPT MY_SCHEMA.AAF_RUN_QUERY_HANDLER('{
    "query_handler": {
        "factory_class": {
            "module": "builtins",
            "name": "CustomQueryHandlerFactory"
        },
        "parameters": "bla-bla",
        "udf": {
            "schema": "MY_SCHEMA",
            "name": "MY_QUERY_HANDLER_UDF"
        }
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": "BFS_CON",
            "directory": "temp"
        },
        "schema_name": "MY_SCHEMA"
    }
}');
