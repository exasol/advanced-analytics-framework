from enum import Enum, auto
from typing import List

import pytest
from typeguard import TypeCheckError

from exasol.analytics.query_handler.graph.stage.sql.data_partition import DataPartition
from exasol.analytics.query_handler.graph.stage.sql.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.dependency import (
    Dependencies,
    Dependency,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
)
from exasol.analytics.schema import (
    Column,
    ColumnName,
    ColumnType,
    SchemaName,
    Table,
    TableNameBuilder,
)


class TestEnum(Enum):
    __test__ = False
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
    name: str, columns: List[Column], dependencies: Dependencies
):
    return DataPartition(
        table_like=Table(
            TableNameBuilder.create(name, SchemaName("TEST_SCHEMA")), columns=columns
        ),
        dependencies=dependencies,
    )


@pytest.fixture()
def dataset(identifier, sample, target):
    partition1 = create_table_data_partition(
        name="TRAIN", columns=[identifier, sample, target], dependencies={}
    )
    dataset = Dataset(
        data_partitions={TestEnum.K1: partition1},
        identifier_columns=[identifier],
        sample_columns=[sample],
        target_columns=[target],
    )
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
    SQLStageInputOutput(
        dataset=dataset, dependencies={TestEnum.K2: Dependency(object="mystr")}
    )
