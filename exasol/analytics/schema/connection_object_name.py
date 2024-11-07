from abc import abstractmethod

from exasol.analytics.schema import DBObjectName



class ConnectionObjectName(DBObjectName):

    @property
    @abstractmethod
    def normalized_name_for_udfs(self) -> str:
        """
        The normalized name of the ConnectionObject
        which can be used in UDFs to retrieve the ConnectionObject
        """
