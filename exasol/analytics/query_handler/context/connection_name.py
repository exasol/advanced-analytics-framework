from typeguard import typechecked

from exasol.analytics.schema import DBObjectName, DBObjectNameImpl


class ConnectionName(DBObjectName):
    """A DBObjectName class which represents the name of a connection object"""

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name.upper())


class ConnectionNameImpl(DBObjectNameImpl, ConnectionName):

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name

    @typechecked
    def __init__(self, connection_name: str):
        super().__init__(connection_name)
