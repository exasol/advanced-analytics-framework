import dataclasses
from abc import ABC
from typing import List, Sized

from exasol.analytics.query_handler.query_handler import QueryHandler
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol.analytics.query_handler.graph.stage.sql.input_output import SQLStageInputOutput


def is_empty(obj: Sized):
    return len(obj) == 0


@dataclasses.dataclass(eq=True)
class SQLStageTrainQueryHandlerInput:
    sql_stage_inputs: List[SQLStageInputOutput]
    result_bucketfs_location: AbstractBucketFSLocation

    def __post_init__(self):
        if is_empty(self.sql_stage_inputs):
            raise AssertionError("Empty sql_stage_inputs not allowed.")


class SQLStageQueryHandler(
    QueryHandler[SQLStageTrainQueryHandlerInput, SQLStageInputOutput], ABC):
    pass
