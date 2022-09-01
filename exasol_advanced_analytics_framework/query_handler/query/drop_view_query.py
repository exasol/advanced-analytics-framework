from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.view_name import ViewName

from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropQuery


class DropViewQuery(DropQuery):

    def __init__(self, view_name: ViewName):
        self._view_name = view_name

    @property
    def query_string(self) -> str:
        return f"DROP VIEW IF EXISTS {self._view_name.fully_qualified};"

    @property
    def view_name(self)-> TableName:
        return self._view_name
