from exasol_data_science_utils_python.preprocessing.sql.schema.table_name import TableName

from exasol_advanced_analytics_framework.query_handler.query.drop_table_query import DropQuery


class DropViewQuery(DropQuery):

    def __init__(self, table_name: TableName):
        self._table_name = table_name

    @property
    def query_string(self) -> str:
        return f"DROP VIEW IF EXISTS {self._table_name.fully_qualified()};"

    @property
    def view_name(self)-> TableName:
        return self._table_name