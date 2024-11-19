from exasol.analytics.query_handler.execution_graph import ExecutionGraph
from exasol.analytics.query_handler.graph.stage.sql.sql_stage import SQLStage

SQLStageGraph = ExecutionGraph[SQLStage]
