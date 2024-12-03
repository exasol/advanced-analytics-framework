from abc import abstractmethod
from typing import Optional

from exasol.analytics.schema.dbobject_name import DBObjectName
from exasol.analytics.schema.schema_name import SchemaName


class DBObjectNameWithSchema(DBObjectName):

    @property
    @abstractmethod
    def schema_name(self) -> Optional[SchemaName]:
        """
        Schema name for the DBObject name
        """
