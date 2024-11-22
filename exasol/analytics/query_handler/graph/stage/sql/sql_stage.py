import abc

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandler,
    SQLStageTrainQueryHandlerInput,
)
from exasol.analytics.query_handler.graph.stage.stage import Stage


class SQLStage(Stage):
    @abc.abstractmethod
    def create_train_query_handler(
        self,
        stage_input: SQLStageTrainQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ) -> SQLStageQueryHandler:
        pass
