import pytest
from pathlib import Path

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory

from tests.utils.parameters import bucketfs_params


@pytest.fixture(scope="session")
def upload_language_container(pyexasol_connection, language_container) -> str:
    bucket_fs_factory = BucketFSFactory()
    container_bucketfs_location = \
        bucket_fs_factory.create_bucketfs_location(
            url=bucketfs_params.address(),
            user=bucketfs_params.user,
            pwd=bucketfs_params.password,
            base_path=None)
    container_path = Path(language_container["container_path"])
    alter_session = language_container["alter_session"]
    language_alias = alter_session.split("=")[0]
    with open(container_path, "rb") as container_file:
        container_bucketfs_location.upload_fileobj_to_bucketfs(
            container_file,
            "exasol_advanced_analytics_framework_container.tar.gz")

    result = pyexasol_connection.execute(
        f"""SELECT "SYSTEM_VALUE" FROM SYS.EXA_PARAMETERS WHERE 
        PARAMETER_NAME='SCRIPT_LANGUAGES'""").fetchall()
    original_alter_system = result[0][0]
    pyexasol_connection.execute(
        f"ALTER SESSION SET SCRIPT_LANGUAGES='{alter_session}'")
    pyexasol_connection.execute(
        f"ALTER SYSTEM SET SCRIPT_LANGUAGES='{alter_session}'")

    yield language_alias
    pyexasol_connection.execute(
        f"ALTER SYSTEM SET SCRIPT_LANGUAGES='{original_alter_system}'")
