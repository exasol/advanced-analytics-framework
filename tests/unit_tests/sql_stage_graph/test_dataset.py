from enum import Enum, auto
from typing import List

import pytest
from exasol.analytics.schema import (
    SchemaName,
    ColumnName,
    Table,
    Column,
    TableNameBuilder,
    ColumnType,
)

from exasol.analytics.query_handler.graph.stage.sql.execution.data_partition import DataPartition
from exasol.analytics.query_handler.graph.stage.sql.execution.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.execution.dependency import Dependencies


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
        name: str,
        columns: List[Column],
        dependencies: Dependencies = None):
    if dependencies is None:
        dependencies = {}
    return DataPartition(
        table_like=Table(
            TableNameBuilder.create(
                name, SchemaName("TEST_SCHEMA")),
            columns=columns),
        dependencies=dependencies
    )


def test_dataset_partitions_with_same_table_like_name(identifier, sample, target):
    extra_column = Column(ColumnName("extra"), ColumnType("INTEGER"))
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample, target, extra_column])
    partition2 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample, target, extra_column])
    with pytest.raises(ValueError, match="The names of table likes of the data partitions should be different."):
        Dataset(data_partitions={TestEnum.K1: partition1,
                                 TestEnum.K2: partition2},
                identifier_columns=[identifier],
                sample_columns=[sample],
                target_columns=[target])


def test_dataset_extra_column_valid(identifier, sample, target):
    extra_column = Column(ColumnName("extra"), ColumnType("INTEGER"))
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample, target, extra_column])
    partition2 = create_table_data_partition(name="TEST",
                                             columns=[identifier, sample, target, extra_column])
    Dataset(data_partitions={TestEnum.K1: partition1,
                             TestEnum.K2: partition2},
            identifier_columns=[identifier],
            sample_columns=[sample],
            target_columns=[target])


def test_dataset_partitions_different_columns_throws_exception(
        identifier, sample, target):
    extra_column = Column(ColumnName("extra"), ColumnType("INTEGER"))
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample, target, extra_column])
    partition2 = create_table_data_partition(name="TEST",
                                             columns=[identifier, sample, target])
    with pytest.raises(ValueError, match="Not all data partitions have the same columns."):
        Dataset(data_partitions={TestEnum.K1: partition1,
                                 TestEnum.K2: partition2},
                identifier_columns=[identifier],
                sample_columns=[sample],
                target_columns=[target])


def test_dataset_not_contains_sample_throws_exception(identifier, sample, target):
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, target])
    partition2 = create_table_data_partition(name="TEST",
                                             columns=[identifier, target])
    with pytest.raises(ValueError, match="Not all sample columns in data partitions."):
        Dataset(data_partitions={TestEnum.K1: partition1,
                                 TestEnum.K2: partition2},
                identifier_columns=[identifier],
                sample_columns=[sample],
                target_columns=[target])


def test_dataset_not_contains_target_throws_exception(identifier, sample, target):
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[identifier, sample])
    partition2 = create_table_data_partition(name="TEST",
                                             columns=[identifier, sample])
    with pytest.raises(ValueError, match="Not all target columns in data partitions."):
        Dataset(data_partitions={TestEnum.K1: partition1,
                                 TestEnum.K2: partition2},
                identifier_columns=[identifier],
                sample_columns=[sample],
                target_columns=[target])


def test_dataset_not_contains_identifier_throws_exception(identifier, sample, target):
    partition1 = create_table_data_partition(name="TRAIN",
                                             columns=[target, sample])
    partition2 = create_table_data_partition(name="TEST",
                                             columns=[target, sample])
    with pytest.raises(ValueError, match="Not all identifier columns in data partitions."):
        Dataset(data_partitions={TestEnum.K1: partition1,
                                 TestEnum.K2: partition2},
                identifier_columns=[identifier],
                sample_columns=[sample],
                target_columns=[target])
