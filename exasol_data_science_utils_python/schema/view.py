from typing import List

from typeguard import typechecked

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.table_like import TableLike
from exasol_data_science_utils_python.schema.view_name import ViewName


class View(TableLike[ViewName]):

    @typechecked
    def __init__(self, name: ViewName, columns: List[Column]):
        super().__init__(name, columns)
