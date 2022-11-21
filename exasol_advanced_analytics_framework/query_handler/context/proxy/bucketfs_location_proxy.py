import logging

from exasol_bucketfs_utils_python.abstract_bucketfs_location import AbstractBucketFSLocation

from exasol_advanced_analytics_framework.query_handler.context.proxy.object_proxy import ObjectProxy

LOGGER = logging.getLogger(__file__)


class BucketFSLocationProxy(ObjectProxy):

    def __init__(self, bucketfs_location: AbstractBucketFSLocation):
        super().__init__()
        self._bucketfs_location = bucketfs_location

    def bucketfs_location(self) -> AbstractBucketFSLocation:
        self._check_if_released()
        return self._bucketfs_location

    def cleanup(self):
        if self._not_released:
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
        except FileNotFoundError as e:
            LOGGER.debug(f"File not found {self._bucketfs_location.get_complete_file_path_in_bucket()} during cleanup.")
        except Exception as e:
            LOGGER.exception(f"Got exception during listing files in temporary BucketFSLocation")
        return files
