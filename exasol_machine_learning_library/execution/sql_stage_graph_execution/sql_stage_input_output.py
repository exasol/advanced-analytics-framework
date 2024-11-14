import dataclasses

from exasol_data_science_utils_python.utils.data_classes_runtime_type_check import check_dataclass_types

from exasol_machine_learning_library.execution.sql_stage_graph_execution.dataset import Dataset
from exasol_machine_learning_library.execution.sql_stage_graph_execution.dependency import Dependencies


@dataclasses.dataclass(frozen=True, eq=True)
class SQLStageInputOutput:
    """
    A SQLStageInputOutput is used as input and output between the SQLStageQueryHandler.
    It contains a dataset and dependencies. The dataset is used to represent train and test data.
    The dependencies can be used to communicate any data to the subsequently stages.
    For example, a dependency could be a table which the previous stage computed and
    the subsequent one uses.
    """

    dataset: Dataset
    dependencies: Dependencies = dataclasses.field(default_factory=dict)
    """
    This contains user-defined dependencies which the previous stage wants to communicate to the subsequent stage.
    """

    def __post_init__(self):
        check_dataclass_types(self)
