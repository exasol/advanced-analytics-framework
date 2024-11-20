import dataclasses

from exasol_bucketfs_utils_python.abstract_bucketfs_location import (
    AbstractBucketFSLocation,
)

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageGraphExecutionInput:
    input: SQLStageInputOutput
    result_bucketfs_location: AbstractBucketFSLocation # should this be bfs.path.PathLike?
    sql_stage_graph: SQLStageGraph
