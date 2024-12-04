import dataclasses
from enum import Enum
from typing import Any, Dict

import typeguard
from typeguard import TypeCheckError

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True)
class Dependency:
    """
    An instance of this class expresses that a object depends on something
    which in fact can depend on something else.  The exact meaning of a
    dependency is user-defined.  For example, a dependency could express that
    a view depends on a certain table.
    """

    object: Any
    dependencies: Dict[Enum, "Dependency"] = dataclasses.field(default_factory=dict)
    """
    Each dependency can again have subsequent dependencies. For example, a
    view can depend on another view which in fact then consists of table.
    """

    def __post_init__(self):
        # We can't use check_dataclass_types(self) here, because the forward definition of "Dependency"
        # can be only resolved if check_type uses the locals and globals of this frame
        try:
            typeguard.check_type(
                value=self.dependencies, expected_type=Dict[Enum, "Dependency"]
            )
        except TypeCheckError as e:
            raise TypeCheckError(f"Field 'dependencies' has wrong type: {e}")


Dependencies = Dict[Enum, Dependency]
