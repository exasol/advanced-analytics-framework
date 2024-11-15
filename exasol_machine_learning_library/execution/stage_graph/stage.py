import abc
from abc import ABC
from typing import Generic, TypeVar

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext

from exasol_machine_learning_library.execution.stage_graph.sql_stage_train_query_handler import \
    SQLStageTrainQueryHandler, SQLStageTrainQueryHandlerInput
from exasol_machine_learning_library.execution.trainable_estimators import Parameter, Result

ParameterType = TypeVar("ParameterType", bound=Parameter)
ResultType = TypeVar("ResultType", bound=Result)


class Stage(ABC, Generic[ParameterType, ResultType]):
    pass


class SQLStage(Stage):
    @abc.abstractmethod
    def create_train_query_handler(self,
                                   stage_input: SQLStageTrainQueryHandlerInput,
                                   query_handler_context: ScopeQueryHandlerContext) \
            -> SQLStageTrainQueryHandler:
        pass


class UDFStage(Stage):
    pass
