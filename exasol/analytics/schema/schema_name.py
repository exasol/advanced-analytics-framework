from typeguard import typechecked

from exasol.analytics.schema import DBObjectNameImpl


class SchemaName(DBObjectNameImpl):

    @typechecked
    def __init__(self, schema_name: str):
        super().__init__(schema_name)

    @property
    def fully_qualified(self) -> str:
        return self.quoted_name
