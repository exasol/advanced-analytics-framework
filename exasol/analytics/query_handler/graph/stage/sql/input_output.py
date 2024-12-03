from dataclasses import dataclass, field
from typing import Dict, Protocol

from exasol.analytics.query_handler.graph.stage.sql.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependencies
# from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclass(frozen=True)
class SQLStageInputOutput:
    """
    A SQLStageInputOutput is used as input and output between the SQLStageQueryHandler.
    It contains a dataset and dependencies. The dataset is used to represent train and test data.
    The dependencies can be used to communicate any data to the subsequently stages.
    For example, a dependency could be a table which the previous stage computed and
    the subsequent one uses.
    """
    pass


@dataclass(frozen=True)
class MultiDatasetSQLStageInputOutput(SQLStageInputOutput):
    datasets: Dict[object, Dataset]
    dependencies: Dependencies = field(default_factory=dict)
