from typing import List

from typeguard import typechecked

from exasol.analytics.schema import (
    Column,
    TableLike,
    ViewName,
)


class View(TableLike[ViewName]):

    @typechecked
    def __init__(self, name: ViewName, columns: List[Column]):
        super().__init__(name, columns)
