import abc
from abc import ABC

from exasol_data_science_utils_python.schema.table_name import TableName
from exasol_data_science_utils_python.schema.udf_name import UDFName
from exasol_data_science_utils_python.schema.view_name import ViewName

from exasol_advanced_analytics_framework.query_handler.context.proxy.bucketfs_location_proxy import \
    BucketFSLocationProxy


class QueryHandlerContext(ABC):
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
    def get_temporary_bucketfs_location(self) -> BucketFSLocationProxy:
        """
        This function registers a new temporary bucketfs file without creating it.
        After the release of this context the framework will remove it.
        """
        pass
