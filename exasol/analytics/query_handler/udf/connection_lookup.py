from exasol.analytics.query_handler.context.scope_query_handler_context import Connection


class UDFConnection(Connection):

    def __init__(self, name: str, udf_connection):
        self._udf_connection = udf_connection
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def address(self) -> str:
        return self._udf_connection.address

    @property
    def user(self) -> str:
        return self._udf_connection.user

    @property
    def password(self) -> str:
        return self._udf_connection.password


class UDFConnectionLookup:
    def __init__(self, exa):
        self.exa = exa

    def __getstate__(self):
        result = self.__dict__.copy()
        del result["exa"]
        return result

    def __call__(self, name: str):
        udf_connection = self.exa.get_connection(name)
        return UDFConnection(name, udf_connection)
