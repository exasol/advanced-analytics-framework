from exasol.analytics.query_handler.context.connection_name import ConnectionName
from exasol.analytics.query_handler.query.drop.interface import DropQuery


class DropConnectionQuery(DropQuery):

    def __init__(self, connection_name: ConnectionName):
        self._connection_name = connection_name

    @property
    def query_string(self) -> str:
        return f"DROP CONNECTION IF EXISTS {self._connection_name.fully_qualified};"

    @property
    def connection_name(self) -> ConnectionName:
        return self._connection_name
