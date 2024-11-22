from typing import List

from typeguard import typechecked

from exasol.analytics.schema.column import Column
from exasol.analytics.schema.table_like import TableLike
from exasol.analytics.schema.view_name import ViewName


class View(TableLike[ViewName]):

    @typechecked
    def __init__(self, name: ViewName, columns: List[Column]):
        super().__init__(name, columns)
