import dataclasses
from abc import ABC
from typing import (
    List,
    Sized,
)

import exasol.bucketfs as bfs

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.query_handler import QueryHandler


def is_empty(obj: Sized):
    return len(obj) == 0


@dataclasses.dataclass(eq=True)
class SQLStageQueryHandlerInput:
    sql_stage_inputs: List[SQLStageInputOutput]
    result_bucketfs_location: bfs.path.PathLike

    def __post_init__(self):
        if is_empty(self.sql_stage_inputs):
            raise AssertionError("Empty sql_stage_inputs not allowed.")


class SQLStageQueryHandler(
    QueryHandler[SQLStageQueryHandlerInput, SQLStageInputOutput], ABC
):
    pass
