import dataclasses
from abc import ABC
from typing import List

from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput
from exasol_machine_learning_library.utils import is_empty


@dataclasses.dataclass(eq=True)
class SQLStageTrainQueryHandlerInput:
    sql_stage_inputs: List[SQLStageInputOutput]
    result_bucketfs_location: AbstractBucketFSLocation

    def __post_init__(self):
        if is_empty(self.sql_stage_inputs):
            raise AssertionError("Empty sql_stage_inputs not allowed.")


class SQLStageTrainQueryHandler(
    QueryHandler[SQLStageTrainQueryHandlerInput, SQLStageInputOutput], ABC):
    pass
