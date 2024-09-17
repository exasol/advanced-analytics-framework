import pytest
import pyexasol # type: ignore
import exasol.bucketfs as bfs

from dataclasses import dataclass
from pathlib import Path
from exasol_advanced_analytics_framework.slc import custom_slc_builder
from exasol.python_extension_common.deployment.language_container_builder import find_path_backwards
from tests.utils.revert_language_settings import revert_language_settings

from exasol_bucketfs_utils_python.bucketfs_factory import BucketFSFactory
from tests.utils.parameters import bucketfs_params


@pytest.fixture(scope="session")
def slc_builder_for_tests():
    test_package = find_path_backwards("tests/test_package", __file__)
    with custom_slc_builder() as builder:
        builder.prepare_flavor(test_package)
        yield builder


# Can be replaced by itde.config.BucketFs as soon as using pytest-plugin pytest-itde
@dataclass
class BucketFsConfig:
    url: str
    username: str
    password: str


def create_container_deployer(language_alias: str, pyexasol_connection: pyexasol.ExaConnection):
    bucketfs_config = BucketFsConfig(
            bucketfs_params.address(),
            bucketfs_params.user,
            bucketfs_params.password,
    )
    bucketfs_path = bfs.path.build_path(
        backend=bfs.path.StorageBackend.onprem,
        url=bucketfs_config.url,
        username=bucketfs_config.username,
        password=bucketfs_config.password,
        service_name="bfsdefault",
        bucket_name="default",
        verify=False,
        # path="container",
    )
    return LanguageContainerDeployer(
        pyexasol_connection,
        language_alias,
        container_bucketfs_location,
    )


@pytest.fixture(scope="session")
# Actually this fixture should not use pyexasol_connection and itde.bucketfs
# but rather something like "backend".
# Do we need to support SaaS as a backend here?
def uploaded_slc(pyexasol_connection, slc_builder_for_tests) -> str:
    builder = slc_builder_for_tests
    builder.build()
    export = builder.export()
    info = export.export_infos[str(builder.flavor_path)]["release"]
    exported_slc_file = Path(info.cache_file)
    alias = builder.language_alias
    deployer = create_container_deployer(alias, pyexasol_connection)
    with revert_language_settings(pyexasol_connection):
        deployer.run(container_file=exported_slc_file, alter_system=True, allow_override=True)
        yield alias


# @pytest.fixture(scope="session")
# def test_package_whl():
#     """
#     Build the wheel file for the the query_handler in the test package.
#     """
#     dir = find_script("tests/test_package")
#     p = subprocess.run(
#         ["poetry", "build"],
#         cwd=dir,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.STDOUT,
#         encoding="UTF-8",
#     )
#     wheels = list((dir / "dist").glob("*.whl"))
#     n = len(wheels)
#     if n != 1:
#         files = ", ".join(str(w) for w in wheels)
#         raise RuntimeError(
#             f"Expected 1 wheel file, while building project"
#             f" in directory {dir} yielded {n} files: {files}.")
#     return wheels[0]


# @pytest.fixture(scope="session")
# def slc_for_tests_1():
#     project_directory = find_path_backwards("pyproject.toml", __file__).parent
#     test_package = find_script("tests/test_package")
#     with custom_slc_builder() as builder:
#         builder.prepare_flavor(project_directory)
#         builder.prepare_flavor(test_package)
#         ii = builder.build()
#         for k,v in ii.items():
#             pprint.pp(vars(v["release"]))
