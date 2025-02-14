from abc import ABC
from typing import (
    Generic,
    TypeVar,
)

from exasol.analytics.query_handler.graph.parameter import Parameter
from exasol.analytics.query_handler.graph.result import Result

ParameterType = TypeVar("ParameterType", bound=Parameter)
ResultType = TypeVar("ResultType", bound=Result)


class Stage(ABC, Generic[ParameterType, ResultType]):
    pass
