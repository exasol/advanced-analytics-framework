import dataclasses

import exasol.bucketfs as bfs

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageGraphExecutionInput:
    """
    The class is an input for the graph query handler (not to be confused with
    a node query handler provided by a user).
    It includes the input data for the root node of the graph, the place in the BucketFS
    where the result should be stored and the graph itself.
    """
    input: SQLStageInputOutput
    result_bucketfs_location: bfs.path.PathLike
    sql_stage_graph: SQLStageGraph
