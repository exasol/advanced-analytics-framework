from abc import abstractmethod

from exasol_data_science_utils_python.schema.dbobject_name import DBObjectName
from exasol_data_science_utils_python.schema.schema_name import SchemaName


class DBObjectNameWithSchema(DBObjectName):

    @property
    @abstractmethod
    def schema_name(self) -> SchemaName:
        """
        Schema name for the DBObject name
        """
