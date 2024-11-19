import dataclasses

from exasol.analytics.schema import TableLike

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types

from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependencies


@dataclasses.dataclass(frozen=True)
class DataPartition:
    """
    A DataPartition is a table_like with dependencies. For example, if the table like is a view,
    the dependencies contain everything which is needed to execute the view, such as tables, udfs,
    connection objects, ....
    """
    table_like: TableLike
    dependencies: Dependencies = dataclasses.field(default_factory=dict)
    """
    This contains user-defined dependencies which are necassary to use the tabe_like. 
    For example, in the case of a view, this could be other views, tables, udfs, ....
    In general, the dependencies should be specific to this partition, because
    new data partition might have this partition as dependency.
    """

    def __post_init__(self):
        check_dataclass_types(self)
