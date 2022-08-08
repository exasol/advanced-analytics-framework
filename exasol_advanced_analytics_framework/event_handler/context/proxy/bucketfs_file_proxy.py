from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy


class BucketFSFileProxy(ObjectProxy):

    def __init__(self, bucketfs_location: BucketFSLocation):
        super().__init__()
        self._bucketfs_location = bucketfs_location

    def bucketfs_location(self) -> BucketFSLocation:
        self._check_if_valid()
        return self._bucketfs_location
