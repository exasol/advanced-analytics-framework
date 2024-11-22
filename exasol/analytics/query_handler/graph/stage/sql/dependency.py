import dataclasses
from enum import Enum
from typing import Any, Dict

import typeguard
from typeguard import TypeCheckError

from exasol.analytics.utils.data_classes_runtime_type_check import check_dataclass_types


@dataclasses.dataclass(frozen=True)
class Dependency:
    """
    This class represents that a object depends on something which in fact can depend on something else.
    That exactly this means is user defined.
    For example, this could represent that a view depends on a certain table.
    """

    object: Any
    dependencies: Dict[Enum, "Dependency"] = dataclasses.field(default_factory=dict)
    """
    Dependency can have their own dependencies. For example, a view which depends on another view
    which in fact then consists of table.
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
