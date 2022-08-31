import abc
from abc import ABC

from exasol_advanced_analytics_framework.query_handler.context.proxy.bucketfs_location_proxy import BucketFSLocationProxy
from exasol_advanced_analytics_framework.query_handler.context.proxy.table_name_proxy import TableNameProxy
from exasol_advanced_analytics_framework.query_handler.context.proxy.view_name_proxy import ViewNameProxy


class QueryHandlerContext(ABC):
    @abc.abstractmethod
    def get_temporary_table_name(self) -> TableNameProxy:
        """
        This function registers a new temporary table without creating it.
        After the release of this context the framework will issue a cleanup query.
        """
        pass

    @abc.abstractmethod
    def get_temporary_view_name(self) -> ViewNameProxy:
        """
        This function registers a new temporary view without creating it.
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

