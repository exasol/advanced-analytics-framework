from typing import Union
from unittest.mock import (
    MagicMock,
    create_autospec,
)

import exasol.bucketfs as bfs
import pytest

from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import (
    SQLStageQueryHandlerInput,
)


def mock_bfs_location() -> Union[bfs.path.PathLike, MagicMock]:
    return create_autospec(bfs.path.PathLike)


def test_empty_stage_inputs():
    bucketfs_location = mock_bfs_location()
    with pytest.raises(AssertionError, match="Empty sql_stage_inputs not allowed."):
        SQLStageQueryHandlerInput(
            sql_stage_inputs=[], result_bucketfs_location=bucketfs_location
        )


def test_non_empty_stage_inputs():
    bucketfs_location = mock_bfs_location()
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(
        SQLStageInputOutput
    )
    obj = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input], result_bucketfs_location=bucketfs_location
    )
    assert (
        obj.sql_stage_inputs == [sql_stage_input]
        and obj.result_bucketfs_location == bucketfs_location
    )


def test_equality():
    bucketfs_location = mock_bfs_location()
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(
        SQLStageInputOutput
    )
    obj1 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input], result_bucketfs_location=bucketfs_location
    )
    obj2 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input], result_bucketfs_location=bucketfs_location
    )
    assert obj1 == obj2


def test_inequality_sql_stage_input():
    bucketfs_location = mock_bfs_location()
    sql_stage_input1: Union[SQLStageInputOutput, MagicMock] = create_autospec(
        SQLStageInputOutput
    )
    sql_stage_input2: Union[SQLStageInputOutput, MagicMock] = create_autospec(
        SQLStageInputOutput
    )
    obj1 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input1], result_bucketfs_location=bucketfs_location
    )
    obj2 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input2], result_bucketfs_location=bucketfs_location
    )
    assert obj1 != obj2


def test_inequality_bucketfs_location():
    bucketfs_location1 = mock_bfs_location()
    bucketfs_location2 = mock_bfs_location()
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(
        SQLStageInputOutput
    )
    obj1 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input], result_bucketfs_location=bucketfs_location1
    )
    obj2 = SQLStageQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input], result_bucketfs_location=bucketfs_location2
    )
    assert obj1 != obj2
