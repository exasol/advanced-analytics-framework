from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy


class BucketFSLocationProxy(ObjectProxy):

    def __init__(self, bucketfs_location: AbstractBucketFSLocation):
        super().__init__()
        self._bucketfs_location = bucketfs_location

    def bucketfs_location(self) -> AbstractBucketFSLocation:
        self._check_if_valid()
        return self._bucketfs_location

    def cleanup(self):
        if self._is_valid:
            raise Exception("Cleanup of BucketFSLocationProxy only allowed after release.")
        for file in self._bucketfs_location.list_files_in_bucketfs(""):
            self._bucketfs_location.delete_file_in_bucketfs(file)