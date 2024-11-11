from abc import abstractmethod
from typing import List

from exasol.analytics.schema import Column


from exasol.analytics.query.handler.query.interface import Query


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
