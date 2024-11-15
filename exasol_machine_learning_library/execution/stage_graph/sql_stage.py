import abc

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol_machine_learning_library.execution.stage_graph.stage import Stage
from exasol_machine_learning_library.execution.stage_graph.sql_stage_train_query_handler import \
    SQLStageTrainQueryHandler, SQLStageTrainQueryHandlerInput

class SQLStage(Stage):
    @abc.abstractmethod
    def create_train_query_handler(
            self,
            stage_input: SQLStageTrainQueryHandlerInput,
            query_handler_context: ScopeQueryHandlerContext,
    ) -> SQLStageTrainQueryHandler:
        pass
