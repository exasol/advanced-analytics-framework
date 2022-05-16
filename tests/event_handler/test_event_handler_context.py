from pathlib import PurePosixPath
from tempfile import TemporaryDirectory
from exasol_udf_mock_python.connection import Connection
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_advanced_analytics_framework.event_handler.event_handler_context \
    import EventHandlerContext


def test_event_handler_context():
    with TemporaryDirectory() as path:
        model_connection = Connection(address=f"file://{path}/data")
        bucketfs_location = BucketFSFactory().create_bucketfs_location(
            url=model_connection.address,
            user=model_connection.user,
            pwd=model_connection.password)

        files_path = PurePosixPath("temporary_path")
        event_handler_context = EventHandlerContext(
            bucketfs_location=bucketfs_location,
            files_path=files_path)

        assert event_handler_context.temporary_bucketfs_file_manager
        assert not event_handler_context.temporary_table_manager
