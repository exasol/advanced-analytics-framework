from enum import Enum, auto
from typing import List

import pytest
from typeguard import TypeCheckError

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table import Table
from exasol_data_science_utils_python.schema.table_name_builder import TableNameBuilder
from exasol_machine_learning_library.execution.sql_stage_graph_execution.data_partition import DataPartition
from exasol_machine_learning_library.execution.sql_stage_graph_execution.dataset import Dataset
from exasol_machine_learning_library.execution.sql_stage_graph_execution.dependency import Dependencies, Dependency
from exasol_machine_learning_library.execution.sql_stage_graph_execution.sql_stage_input_output import \
    SQLStageInputOutput


class TestEnum(Enum):
    K1 = auto()
    K2 = auto()


@pytest.fixture()
def identifier():
    identifier = Column(ColumnName("id"), ColumnType("INTEGER"))
    return identifier


@pytest.fixture()
def sample():
    sample = Column(ColumnName("x1"), ColumnType("INTEGER"))
    return sample


@pytest.fixture()
def target():
    target = Column(ColumnName("y1"), ColumnType("INTEGER"))
    return target


def create_table_data_partition(
        name: str,
        columns: List[Column],
        dependencies: Dependencies):
    return DataPartition(
        table_like=Table(
            TableNameBuilder.create(
                name, SchemaName("TEST_SCHEMA")),
            columns=columns),
        dependencies=dependencies
    )


@pytest.fixture()
def dataset(identifier, sample, target):
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample, target],
                                             dependencies={})
    dataset = Dataset(data_partitions={TestEnum.K1: partition1},
                      identifier_columns=[identifier],
                      sample_columns=[sample],
                      target_columns=[target])
    return dataset


def test_no_dataset(dataset):
    with pytest.raises(TypeError):
        SQLStageInputOutput()


def test_none_dataset(dataset):
    with pytest.raises(TypeCheckError):
        SQLStageInputOutput(dataset=None)


def test_dataset(dataset):
    SQLStageInputOutput(dataset=dataset)


def test_dependencies(dataset):
    SQLStageInputOutput(dataset=dataset, dependencies={TestEnum.K2: Dependency(object="mystr")})
