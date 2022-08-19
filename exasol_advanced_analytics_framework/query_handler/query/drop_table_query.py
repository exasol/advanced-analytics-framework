from exasol_data_science_utils_python.preprocessing.sql.schema.table_name import TableName

from exasol_advanced_analytics_framework.query_handler.query.query import Query


class DropTableQuery(Query):

    def __init__(self, table_name: TableName):
        self._table_name = table_name

    def get_query_str(self) -> str:
        return f"DROP TABLE IF EXISTS {self._table_name.fully_qualified()};"
