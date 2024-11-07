from typing import Union, Optional

from exasol.analytics.schema import (
    ConnectionObjectName,
    SchemaName,
    ConnectionObjectNameImpl,
    TableName,
    ViewNameImpl,
    TableNameImpl,
    ViewName,
)


class ConnectionObjectNameBuilder:

    def __init__(self, name: str):
        self._name = name

    def build(self) -> ConnectionObjectName:
        return self.create(self._name)

    @classmethod
    def create(cls, name: str):
        return ConnectionObjectNameImpl(name)
