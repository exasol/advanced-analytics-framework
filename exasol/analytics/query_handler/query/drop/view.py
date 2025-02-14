from exasol.analytics.query_handler.query.drop.interface import DropQuery
from exasol.analytics.schema import (
    TableName,
    ViewName,
)


class DropViewQuery(DropQuery):

    def __init__(self, view_name: ViewName):
        self._view_name = view_name

    @property
    def query_string(self) -> str:
        return f"DROP VIEW IF EXISTS {self._view_name.fully_qualified};"

    @property
    def view_name(self) -> ViewName:
        return self._view_name
