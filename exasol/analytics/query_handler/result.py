from dataclasses import dataclass
from typing import (
    Generic,
    List,
    TypeVar,
)

from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition
from exasol.analytics.query_handler.audit.query_handler_audit import QueryHandlerAudit


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
    audit: QueryHandlerAudit | None = None
