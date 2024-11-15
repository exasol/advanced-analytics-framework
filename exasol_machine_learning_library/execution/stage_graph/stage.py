from abc import ABC
from typing import Generic, TypeVar

from exasol_machine_learning_library.execution.trainable_estimators import Parameter, Result

ParameterType = TypeVar("ParameterType", bound=Parameter)
ResultType = TypeVar("ResultType", bound=Result)


class Stage(ABC, Generic[ParameterType, ResultType]):
    pass
