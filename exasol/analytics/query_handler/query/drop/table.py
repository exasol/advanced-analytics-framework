from exasol.analytics.schema import TableName


from exasol.analytics.query_handler.query.drop.interface import DropQuery


class DropTableQuery(DropQuery):

    def __init__(self, table_name: TableName):
        self._table_name = table_name

    @property
    def query_string(self) -> str:
        return f"DROP TABLE IF EXISTS {self._table_name.fully_qualified};"

    @property
    def table_name(self) -> TableName:
        return self._table_name
