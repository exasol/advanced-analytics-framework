from abc import abstractmethod

from exasol.analytics.schema import DBObjectName, SchemaName


class DBObjectNameWithSchema(DBObjectName):

    @property
    @abstractmethod
    def schema_name(self) -> SchemaName:
        """
        Schema name for the DBObject name
        """
