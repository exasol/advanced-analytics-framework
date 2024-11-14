import dataclasses

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_machine_learning_library.execution.sql_stage_graph.sql_stage_graph import SQLStageGraph
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageGraphExecutionInput:
    input: SQLStageInputOutput
    result_bucketfs_location: AbstractBucketFSLocation
    sql_stage_graph: SQLStageGraph
