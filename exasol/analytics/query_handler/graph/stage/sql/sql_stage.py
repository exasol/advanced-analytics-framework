import abc

from exasol.analytics.query_handler.context.scope import ScopeQueryHandlerContext
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandler,
    SQLStageQueryHandlerInput,
)
from exasol.analytics.query_handler.graph.stage.stage import Stage


class SQLStage(Stage):
    """
    This is a node of an ExecutionGraph.
    Essentially, this is a node-level query handler factory. The query handler
    itself is user-provided and so is this factory.
    """

    @abc.abstractmethod
    def create_query_handler(
        self,
        stage_input: SQLStageQueryHandlerInput,
        query_handler_context: ScopeQueryHandlerContext,
    ) -> SQLStageQueryHandler:
        pass
