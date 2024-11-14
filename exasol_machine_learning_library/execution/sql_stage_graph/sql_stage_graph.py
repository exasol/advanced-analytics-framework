from typing import Set, List

from exasol_advanced_analytics_framework.query_handler.context.scope_query_handler_context import \
    ScopeQueryHandlerContext

from exasol_machine_learning_library.execution.execution_graph import ExecutionGraph
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput
from exasol_machine_learning_library.execution.stage_graph.stage import UDFStage, SQLStage
from exasol_machine_learning_library.execution.stage_graph.sql_stage_train_query_handler import \
    SQLStageTrainQueryHandler

UDFStageGraph = ExecutionGraph[UDFStage]
SQLStageGraph = ExecutionGraph[SQLStage]


class UDFRunnerPlaceholderSQLStage(SQLStage):
    def __init__(self, udf_stages_component: Set[UDFStage]):
        self._udf_stage_component = udf_stages_component

    @property
    def conntected_udf_stages(self) -> Set[UDFStage]:
        return set(self._udf_stage_component)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}@{id(self)}"

    def create_train_query_handler(self, stage_inputs: List[SQLStageInputOutput],
                                   query_handler_context: ScopeQueryHandlerContext) -> SQLStageTrainQueryHandler:
        raise NotImplemented("The method create_train_query_handler should never be called on this stage, "
                             "because it is a placeholder for graph rewriting.")


class UDFRunnerSQLStage(SQLStage):
    def __init__(self, udf_stage_graph: UDFStageGraph):
        self._udf_stage_graph = udf_stage_graph

    @property
    def udf_stage_graph(self) -> UDFStageGraph:
        return self._udf_stage_graph

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}@{id(self)}"

    def create_train_query_handler(self, stage_inputs: List[SQLStageInputOutput],
                                   query_handler_context: ScopeQueryHandlerContext) -> SQLStageTrainQueryHandler:
        raise NotImplemented("The create_train_query_handler method needs a implementation.")


class ColumnSelectorPlaceholderUDFStage(UDFStage):
    def __init__(self, input_sql_stage: List[SQLStage]):
        self._input_sql_stages = input_sql_stage

    @property
    def input_sql_stages(self) -> List[SQLStage]:
        return self._input_sql_stages

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}@{id(self)}"


class SourceUDFStage(UDFStage):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}@{id(self)}"


class SinkUDFStage(UDFStage):
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}@{id(self)}"
