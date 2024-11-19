import dataclasses
from enum import Enum
from typing import Dict, List, Tuple, Union

from exasol.analytics.query_handler.graph.stage.sql.data_partition import DataPartition
from exasol.analytics.schema import Column
from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types

DataPartitionName = Union[Enum, Tuple[Enum, int]]


@dataclasses.dataclass(frozen=True)
class Dataset:
    """
    A Dataset consists of multiple data partitions and column lists which indicate the identifier,
    sample and target columns, The data paritions can be used to describe train and test sets.
    """

    data_partitions: Dict[DataPartitionName, DataPartition]
    identifier_columns: List[Column]
    sample_columns: List[Column]
    target_columns: List[Column]

    def __post_init__(self):
        check_dataclass_types(self)
        self._check_table_name()
        self._check_columns()

    def _check_table_name(self):
        all_table_like_names = {
            data_partition.table_like.name
            for data_partition in self.data_partitions.values()
        }
        if len(all_table_like_names) != len(self.data_partitions):
            raise ValueError(
                "The names of table likes of the data partitions should be different."
            )

    def _check_columns(self):
        all_columns = {
            column
            for data_partition in self.data_partitions.values()
            for column in data_partition.table_like.columns
        }
        all_data_partition_have_same_columns = all(
            len(data_partition.table_like.columns) == len(all_columns)
            for data_partition in self.data_partitions.values()
        )
        if not all_data_partition_have_same_columns:
            raise ValueError("Not all data partitions have the same columns.")
        if not all_columns.issuperset(self.sample_columns):
            raise ValueError("Not all sample columns in data partitions.")
        if not all_columns.issuperset(self.target_columns):
            raise ValueError("Not all target columns in data partitions.")
        if not all_columns.issuperset(self.identifier_columns):
            raise ValueError("Not all identifier columns in data partitions.")
