from exasol_machine_learning_library.execution.execution_graph import ExecutionGraph
from exasol_machine_learning_library.execution.stage_graph.sql_stage import SQLStage

SQLStageGraph = ExecutionGraph[SQLStage]
