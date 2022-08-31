import json
from pathlib import PurePosixPath
from typing import Union

import pytest
from exasol_bucketfs_utils_python.localfs_mock_bucketfs_location import LocalFSMockBucketFSLocation
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.context.top_level_query_handler_context import \
    TopLevelQueryHandlerContext
from exasol_advanced_analytics_framework.query_handler.json_udf_query_handler import JSONQueryHandler, JSONType
from exasol_advanced_analytics_framework.query_handler.result import Continue, Finish
from exasol_advanced_analytics_framework.query_result.mock_query_result import MockQueryResult
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.udf_framework.json_udf_query_handler_factory import JsonUDFQueryHandlerFactory
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler


@pytest.fixture()
def temporary_schema_name():
    return "temp_schema_name"


@pytest.fixture()
def top_level_query_handler_context(tmp_path, temporary_schema_name):
    top_level_query_handler_context = TopLevelQueryHandlerContext(
        temporary_bucketfs_location=LocalFSMockBucketFSLocation(base_path=PurePosixPath(tmp_path) / "bucketfs"),
        temporary_db_object_name_prefix="temp_db_object",
        temporary_schema_name=temporary_schema_name,
    )
    return top_level_query_handler_context


class TestJSONQueryHandler(JSONQueryHandler):
    def __init__(self, parameter: JSONType, query_handler_context: ScopeQueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self._parameter = parameter

    def start(self) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameter)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[JSONType]]:
        return Finish[JSONType](self._parameter)


class TestJsonUDFQueryHandlerFactory(JsonUDFQueryHandlerFactory):
    def __init__(self):
        super().__init__(TestJSONQueryHandler)


def test(top_level_query_handler_context):
    test_input = {"a": 1}
    json_str = json.dumps(test_input)
    query_handler = TestJsonUDFQueryHandlerFactory().create(json_str, top_level_query_handler_context)
    start_result = query_handler.start()
    handle_query_result = query_handler.handle_query_result(
        MockQueryResult(data=[(1,)],
                        columns=[Column(ColumnName("a"),
                                        ColumnType("INTEGER"))])
    )
    assert isinstance(query_handler, UDFQueryHandler) \
           and start_result.result == json_str and handle_query_result.result == json_str
