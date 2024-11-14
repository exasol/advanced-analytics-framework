import abc
from abc import ABC

from exasol.analytics.schema import (
    TableName,
    UDFName,
    ViewName,
)

from exasol.analytics.query_handler.context.connection_name import ConnectionName
from exasol.analytics.query_handler.context.proxy.bucketfs_location_proxy import     BucketFSLocationProxy


class QueryHandlerContext(ABC):

    @abc.abstractmethod
    def get_temporary_name(self) -> str:
        """
        Returns a temporary name
        """
        pass

    @abc.abstractmethod
    def get_temporary_table_name(self) -> TableName:
        """
        This function registers a new temporary table without creating it.
        After the release of this context the framework will issue a cleanup query.
        """
        pass

    @abc.abstractmethod
    def get_temporary_view_name(self) -> ViewName:
        """
        This function registers a new temporary view without creating it.
        After the release of this context the framework will issue a cleanup query.
        """

        pass

    @abc.abstractmethod
    def get_temporary_udf_name(self) -> UDFName:
        """
        This function registers a new temporary script without creating it.
        After the release of this context the framework will issue a cleanup query.
        """

        pass

    @abc.abstractmethod
    def get_temporary_connection_name(self) -> ConnectionName:
        """
        This function registers a new temporary connection without creating it.
        After the release of this context the framework will issue a cleanup query.
        """

        pass

    @abc.abstractmethod
    def get_temporary_bucketfs_location(self) -> BucketFSLocationProxy:
        """
        This function registers a new temporary bucketfs file without creating it.
        After the release of this context the framework will remove it.
        """
        pass
