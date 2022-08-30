from abc import abstractmethod
from typing import List

from exasol_data_science_utils_python.schema.column import Column

from exasol_advanced_analytics_framework.query_handler.query.query import Query


class SelectQuery(Query):

    def __init__(self, query_string: str):
        self._query_string = query_string

    @property
    def query_string(self) -> str:
        return self._query_string


class SelectQueryWithColumnDefinition(SelectQuery):

    def __init__(self, query_string: str, output_columns: List[Column]):
        super().__init__(query_string)
        self._output_columns = output_columns

    @property
    def output_columns(self) -> List[Column]:
        return self._output_columns
