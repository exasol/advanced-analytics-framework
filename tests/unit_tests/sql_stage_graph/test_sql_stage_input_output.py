from enum import Enum, auto
from typing import List

import pytest
from typeguard import TypeCheckError

from exasol.analytics.query_handler.graph.stage.sql.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.dependency import (
    Dependencies,
    Dependency,
)
from exasol.analytics.query_handler.graph.stage.sql.input_output import (
    SQLStageInputOutput,
    MultiDatasetSQLStageInputOutput,
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


def create_table_like(name: str, columns: List[Column]):
    return Table(
        TableNameBuilder.create(name, SchemaName("TEST_SCHEMA")),
        columns=columns,
    )


@pytest.fixture()
def dataset(identifier, sample, target):
    table_like = create_table_like(name="TRAIN", columns=[identifier, sample, target])
    dataset = Dataset(
        table_like=table_like,
        identifier_columns=[identifier],
        columns=[sample, target],
    )
    return dataset


@pytest.fixture()
def datasets(dataset):
    return { "TRAIN": dataset }


def test_no_dataset():
    with pytest.raises(TypeError):
        MultiDatasetSQLStageInputOutput()


@pytest.mark.skip("Runtime type check have been removed")
def test_datasets_None():
    with pytest.raises(TypeCheckError):
        MultiDatasetSQLStageInputOutput(datasets=None)


def test_dataset(datasets):
    MultiDatasetSQLStageInputOutput(datasets=datasets)


def test_dependencies(datasets):
    MultiDatasetSQLStageInputOutput(
        datasets=datasets, dependencies={TestEnum.K2: Dependency(object="mystr")}
    )
