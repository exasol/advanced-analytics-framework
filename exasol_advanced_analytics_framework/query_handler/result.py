from dataclasses import dataclass
from typing import List, Generic, TypeVar

from exasol_advanced_analytics_framework.query_handler.query.query import Query
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQueryWithColumnDefinition


@dataclass()
class Result:
    pass


@dataclass()
class Continue(Result):
    query_list: List[Query]
    input_query: SelectQueryWithColumnDefinition


T = TypeVar("T")


@dataclass()
class Finish(Generic[T], Result):
    result: T
