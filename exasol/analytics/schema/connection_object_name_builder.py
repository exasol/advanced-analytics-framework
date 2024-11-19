from typing import Optional, Union

from exasol.analytics.schema import (
    ConnectionObjectName,
    ConnectionObjectNameImpl,
    SchemaName,
    TableName,
    TableNameImpl,
    ViewName,
    ViewNameImpl,
)


class ConnectionObjectNameBuilder:

    def __init__(self, name: str):
        self._name = name

    def build(self) -> ConnectionObjectName:
        return self.create(self._name)

    @classmethod
    def create(cls, name: str):
        return ConnectionObjectNameImpl(name)
