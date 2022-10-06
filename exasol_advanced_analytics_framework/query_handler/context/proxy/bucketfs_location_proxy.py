import logging

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.query_handler.context.proxy.object_proxy import ObjectProxy

LOGGER = logging.getLogger(__file__)


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
        files = self._list_files()
        for file in files:
            self._remove_file(file)

    def _remove_file(self, file):
        try:
            self._bucketfs_location.delete_file_in_bucketfs(file)
        except Exception as e:
            LOGGER.error(f"Failed to remove {file}, got exception", exc_info=True)

    def _list_files(self):
        files = []
        try:
            files = self._bucketfs_location.list_files_in_bucketfs("")
        except Exception as e:
            LOGGER.debug(f"Got exception during listing files in temporary BucketFSLocation", exc_info=True)
        return files
