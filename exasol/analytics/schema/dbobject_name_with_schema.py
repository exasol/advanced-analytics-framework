from abc import abstractmethod

from exasol.analytics.schema.dbobject_name import DBObjectName
from exasol.analytics.schema.schema_name import SchemaName


class DBObjectNameWithSchema(DBObjectName):

    @property
    @abstractmethod
    def schema_name(self) -> SchemaName:
        """
        Schema name for the DBObject name
        """
