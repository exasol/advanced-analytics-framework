from dataclasses import dataclass, field
from typing import Dict, Protocol

from exasol.analytics.query_handler.graph.stage.sql.dataset import Dataset
from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependencies


@dataclass(frozen=True)
class SQLStageInputOutput:
    """
    This is a type root for a class representing input/output data for a customer
    provided node-level query handler extending class SQLStageQueryHandler. The actual content of
    the input/output is application-specific.
    """
    pass


@dataclass(frozen=True)
class MultiDatasetSQLStageInputOutput(SQLStageInputOutput):
    """
    An implementation of SQLStageInputOutput holding a collection of datasets and
    dependencies. Can be used, for example, for model training. The datasets may be
    used to represent train and test data. The dependencies can be used to communicate
    any data to the subsequently stages. For example, a dependency could be a table
    which the previous stage computed and the subsequent one uses.
    """
    datasets: Dict[object, Dataset]
    dependencies: Dependencies = field(default_factory=dict)
