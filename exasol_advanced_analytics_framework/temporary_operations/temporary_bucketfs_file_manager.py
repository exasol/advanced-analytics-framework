import typing
from pathlib import PurePosixPath
from exasol_bucketfs_utils_python.bucketfs_location import BucketFSLocation


class TemporaryBucketFSFileManager:
    def __init__(self, bucketfs_location: BucketFSLocation,
                 bucketfs_path: PurePosixPath):
        self.bucketfs_location = bucketfs_location
        self.bucketfs_path = bucketfs_path

    def save_object(self, object_to_load: typing.Any, file_name: str) -> None:
        self.bucketfs_location.upload_object_to_bucketfs_via_joblib(
            object_to_load, str(PurePosixPath(self.bucketfs_path, file_name)))

    def load_object(self, file_name: str) -> typing.Any:
        self.bucketfs_location.download_object_from_bucketfs_via_joblib(
            str(PurePosixPath(self.bucketfs_path, file_name)))

    def save_fileobj(self, fileobj: typing.IO, file_name: str ) -> None:
        self.bucketfs_location.upload_fileobj_to_bucketfs(
            fileobj, str(PurePosixPath(self.bucketfs_path, file_name)))

    def load_fileobj(self, file_name: str, fileobj: typing.IO) -> None:
        self.bucketfs_location.read_file_from_bucketfs_to_fileobj(
            str(PurePosixPath(self.bucketfs_path, file_name)), fileobj)
