import dataclasses

import exasol.bucketfs as bfs

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageGraphExecutionInput:
    input: SQLStageInputOutput
    result_bucketfs_location: bfs.path.PathLike
    sql_stage_graph: SQLStageGraph
