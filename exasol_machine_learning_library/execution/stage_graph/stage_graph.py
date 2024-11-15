from exasol_machine_learning_library.execution.execution_graph import ExecutionGraph
from exasol_machine_learning_library.execution.stage_graph.stage import SQLStage, UDFStage, Stage

StageGraph = ExecutionGraph[Stage]
