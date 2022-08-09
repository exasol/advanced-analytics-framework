import abc
from abc import ABC

from exasol_advanced_analytics_framework.event_handler.context.proxy.bucketfs_location_proxy import BucketFSLocationProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.table_proxy import TableProxy
from exasol_advanced_analytics_framework.event_handler.context.proxy.view_proxy import ViewProxy


class EventHandlerContext(ABC):
    @abc.abstractmethod
    def get_temporary_table(self) -> TableProxy:
        pass

    @abc.abstractmethod
    def get_temporary_view(self) -> ViewProxy:
        pass

    @abc.abstractmethod
    def get_temporary_bucketfs_file(self) -> BucketFSLocationProxy:
        pass

