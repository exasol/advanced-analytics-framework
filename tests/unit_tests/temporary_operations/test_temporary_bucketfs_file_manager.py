from pathlib import PurePosixPath
from tempfile import TemporaryDirectory, NamedTemporaryFile
from exasol_udf_mock_python.connection import Connection
from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from exasol_advanced_analytics_framework.event_handler.context.event_handler_context import \
    EventHandlerContext


def _create_event_handler_context(
        model_connection: Connection) -> EventHandlerContext:
    bucketfs_location = BucketFSFactory().create_bucketfs_location(
        url=model_connection.address,
        user=model_connection.user,
        pwd=model_connection.password
    )

    files_path = PurePosixPath("temporary_path")
    event_handler_context = EventHandlerContext(
        bucketfs_location=bucketfs_location,
        files_path=files_path)
    return event_handler_context


def test_object_operations_via_temporary_bucketfs_file_manager():
    with TemporaryDirectory() as path:
        model_connection = Connection(address=f"file://{path}/data")
        event_handler_context = _create_event_handler_context(model_connection)

        tmp_data = [1, 2, 3]
        tmp_data_fname = "tmp_data_path"
        event_handler_context\
            .temporary_bucketfs_file_manager\
            .save_object(tmp_data, tmp_data_fname)

        loaded_tmp_data = event_handler_context\
            .temporary_bucketfs_file_manager\
            .load_object(tmp_data_fname)

        assert tmp_data == loaded_tmp_data


def test_fileobject_operations_via_temporary_bucketfs_file_manager():
    tmp_file_fname = "tmp_file_path.txt"
    input_test_byte_string = b"test_byte_string"

    with TemporaryDirectory() as path:
        model_connection = Connection(address=f"file://{path}/data")
        event_handler_context = _create_event_handler_context(model_connection)

        with NamedTemporaryFile() as input_tmp_file:
            input_tmp_file.write(input_test_byte_string)
            input_tmp_file.flush()
            input_tmp_file.seek(0)

            event_handler_context \
                .temporary_bucketfs_file_manager \
                .save_fileobj(input_tmp_file, tmp_file_fname)

        with NamedTemporaryFile() as output_tmp_file:
            event_handler_context \
                .temporary_bucketfs_file_manager \
                .load_fileobj(tmp_file_fname, output_tmp_file)

            output_tmp_file.flush()
            output_tmp_file.seek(0)
            output_test_byte_string = output_tmp_file.read()

    assert input_test_byte_string == output_test_byte_string
