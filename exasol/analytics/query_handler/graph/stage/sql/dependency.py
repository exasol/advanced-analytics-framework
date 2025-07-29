import dataclasses
from enum import Enum
from typing import (
    Any,
    Dict,
)

import typeguard
from typeguard import TypeCheckError

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True)
class Dependency:
    """
    An instance of this class represents a node in a dependency graph.

    The exact meaning of a dependency is user-defined.  For example, a
    dependency could express that a database view depends on a particular
    table.
    """

    object: Any
    dependencies: dict[Enum, "Dependency"] = dataclasses.field(default_factory=dict)
    """
    Each dependency can again have subsequent dependencies. For example, a
    view can depend on another view which in fact then consists of table.
    """

    def __post_init__(self):
        # We can't use check_dataclass_types(self) here, because the forward definition of "Dependency"
        # can be only resolved if check_type uses the locals and globals of this frame
        try:
            typeguard.check_type(
                value=self.dependencies, expected_type=dict[Enum, "Dependency"]
            )
        except TypeCheckError as e:
            raise TypeCheckError(f"Field 'dependencies' has wrong type: {e}")


Dependencies = dict[object, Dependency]
