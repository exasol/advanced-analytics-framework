from exasol_advanced_analytics_framework.event_handler.query.query import Query


class DropTableQuery(Query):

    def __init__(self, table_name: str):
        self._table_name = table_name

    def get_query_str(self) -> str:
        return f"DROP TABLE {self._table_name};"
