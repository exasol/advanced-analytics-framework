from typing import List

from typeguard import typechecked

from exasol.analytics.schema import (
    Column,
    TableName,
    TableLike,
)


class Table(TableLike[TableName]):

    @typechecked
    def __init__(self, name: TableName, columns: List[Column]):
        super().__init__(name, columns)