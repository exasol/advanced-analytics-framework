import dataclasses

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol.analytics.query_handler.graph.stage.sql.sql_stage_graph import SQLStageGraph
from exasol.analytics.query_handler.graph.stage.sql.execution.input_output import SQLStageInputOutput


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageGraphExecutionInput:
    input: SQLStageInputOutput
    result_bucketfs_location: AbstractBucketFSLocation
    sql_stage_graph: SQLStageGraph
