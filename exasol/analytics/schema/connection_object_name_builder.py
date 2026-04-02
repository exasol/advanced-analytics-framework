from exasol.analytics.schema.connection_object_name import ConnectionObjectName
from exasol.analytics.schema.connection_object_name_impl import ConnectionObjectNameImpl


class ConnectionObjectNameBuilder:

    def __init__(self, name: str):
        self._name = name

    def build(self) -> ConnectionObjectName:
        return self.create(self._name)

    @classmethod
    def create(cls, name: str):
        return ConnectionObjectNameImpl(name)
