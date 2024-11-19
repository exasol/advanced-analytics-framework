from typing import Union
from unittest.mock import create_autospec, MagicMock

import pytest
from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol.analytics.query_handler.graph.stage.sql.input_output import SQLStageInputOutput
from exasol.analytics.query_handler.graph.stage.sql.sql_stage_query_handler import SQLStageTrainQueryHandlerInput


def test_empty_stage_inputs():
    bucketfs_location: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    with pytest.raises(AssertionError, match="Empty sql_stage_inputs not allowed."):
        SQLStageTrainQueryHandlerInput(
            sql_stage_inputs=[],
            result_bucketfs_location=bucketfs_location
        )


def test_non_empty_stage_inputs():
    bucketfs_location: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(SQLStageInputOutput)
    obj = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input],
        result_bucketfs_location=bucketfs_location
    )
    assert (
            obj.sql_stage_inputs == [sql_stage_input]
            and obj.result_bucketfs_location == bucketfs_location
    )


def test_equality():
    bucketfs_location: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(SQLStageInputOutput)
    obj1 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input],
        result_bucketfs_location=bucketfs_location
    )
    obj2 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input],
        result_bucketfs_location=bucketfs_location
    )
    assert obj1 == obj2

def test_inequality_sql_stage_input():
    bucketfs_location: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    sql_stage_input1: Union[SQLStageInputOutput, MagicMock] = create_autospec(SQLStageInputOutput)
    sql_stage_input2: Union[SQLStageInputOutput, MagicMock] = create_autospec(SQLStageInputOutput)
    obj1 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input1],
        result_bucketfs_location=bucketfs_location
    )
    obj2 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input2],
        result_bucketfs_location=bucketfs_location
    )
    assert obj1 != obj2

def test_inequality_bucketfs_location():
    bucketfs_location1: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    bucketfs_location2: Union[AbstractBucketFSLocation, MagicMock] = create_autospec(AbstractBucketFSLocation)
    sql_stage_input: Union[SQLStageInputOutput, MagicMock] = create_autospec(SQLStageInputOutput)
    obj1 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input],
        result_bucketfs_location=bucketfs_location1
    )
    obj2 = SQLStageTrainQueryHandlerInput(
        sql_stage_inputs=[sql_stage_input],
        result_bucketfs_location=bucketfs_location2
    )
    assert obj1 != obj2