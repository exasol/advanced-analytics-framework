from exasol_advanced_analytics_framework.temporary_operations.temporary_bucketfs_file_manager import \
    TemporaryBucketFSFileManager
from exasol_advanced_analytics_framework.temporary_operations.temporary_table_manager import \
    TemporaryTableManager


class EventHandlerContext:
    def __init__(self, bucketfs_connection):
        self._temporary_table_manager = None
        self._temporary_bucketfs_file_manager = None

    @property
    def temporary_table_manager(self) -> TemporaryTableManager:
        return self._temporary_table_manager

    @property
    def temporary_bucketfs_file_manager(self) -> TemporaryBucketFSFileManager:
        return self._temporary_bucketfs_file_manager
