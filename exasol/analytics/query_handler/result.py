from dataclasses import dataclass
from typing import List, Generic, TypeVar

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition


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
