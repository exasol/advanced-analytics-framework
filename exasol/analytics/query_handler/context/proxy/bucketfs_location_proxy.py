import logging # xtype: ignore[import-untyped]

import exasol.bucketfs as bfs

from exasol.analytics.query_handler.context.proxy.object_proxy import ObjectProxy

LOGGER = logging.getLogger(__file__)


class BucketFSLocationProxy(ObjectProxy):

    def __init__(self, bucketfs_location: bfs.path.PathLike):
        super().__init__()
        self._bucketfs_location = bucketfs_location

    def bucketfs_location(self) -> bfs.path.PathLike:
        self._check_if_released()
        return self._bucketfs_location

    def cleanup(self):
        if self._not_released:
            raise Exception(
                "Cleanup of BucketFSLocationProxy only allowed after release."
            )
        files = self._list_files()
        for file in files:
            self._remove_file(file)

    def _remove_file(self, file):
        try:
            file.rm()
        except Exception as e:
            LOGGER.error(f"Failed to remove {file}, got exception", exc_info=True)

    def _list_files(self):
        try:
            return list(self._bucketfs_location.iterdir())
        except FileNotFoundError as e:
            LOGGER.debug(
                f"File not found {self._bucketfs_location.as_udf_path} during cleanup."
            )
        except Exception as e:
            LOGGER.exception(
                f"Got exception during listing files in temporary BucketFSLocation"
            )
        return []
