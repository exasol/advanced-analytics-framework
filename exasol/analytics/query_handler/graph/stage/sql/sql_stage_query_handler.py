import dataclasses
from abc import ABC
from typing import List, Sized

from exasol_bucketfs_utils_python.abstract_bucketfs_location import (
    AbstractBucketFSLocation,
)

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.query_handler import QueryHandler


def is_empty(obj: Sized):
    return len(obj) == 0


@dataclasses.dataclass(eq=True)
class SQLStageQueryHandlerInput:
    """
    A Parameter type for the SQLStageQueryHandler.
    Contains a list of Input/Output objects and a location in the BucketFS for
    persisting the data if needed.

    Why is the list? Because the input data for a node can come from multiple
    predecessors.
    """
    sql_stage_inputs: List[SQLStageInputOutput]
    result_bucketfs_location: AbstractBucketFSLocation

    def __post_init__(self):
        if is_empty(self.sql_stage_inputs):
            raise AssertionError("Empty sql_stage_inputs are not allowed.")


class SQLStageQueryHandler(
    QueryHandler[SQLStageQueryHandlerInput, SQLStageInputOutput], ABC
):
    """
    The base class for a QueryHandler of a single node. The implementation shall be
    provided by the user. See the UDFQueryHandler and the user_guide.md for details
    and guidelines on how to build a query handler.
    """
    pass
