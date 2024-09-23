import pytest

from exasol_advanced_analytics_framework.slc import custom_slc_builder
from exasol.python_extension_common.deployment.language_container_builder import (
    find_path_backwards,
    LanguageContainerBuilder,
)


@pytest.fixture(scope="session")
def slc_builder(use_onprem, use_saas) -> LanguageContainerBuilder:
    """
    Overrides default definition from pytest-exasol-slc.
    """
    if use_saas or use_onprem:
        test_package = find_path_backwards("tests/test_package", __file__)
        with custom_slc_builder() as builder:
            builder.prepare_flavor(test_package)
            yield builder
    else:
        yield None

import exasol.bucketfs as bfs
from exasol.python_extension_common.deployment.language_container_deployer import LanguageContainerDeployer
BFS_CONTAINER_DIRECTORY = 'container'
# can be removed as soon as the following issues is fixed and
# a new version of PYTSLC is released
# https://github.com/exasol/pytest-plugins/issues/58
@pytest.fixture(scope="session")
def upload_slc(slc_builder, export_slc, pyexasol_connection, backend_aware_bucketfs_params):
    """
    The fixture uploads language container to a database, according to the selected
    backends.
    """
    if (slc_builder is not None) and (export_slc is not None):
        # pyexasol_connection = pyexasol.connect(**backend_aware_database_params)
        bucketfs_path = bfs.path.build_path(**backend_aware_bucketfs_params,
                                            path=BFS_CONTAINER_DIRECTORY)
        deployer = LanguageContainerDeployer(pyexasol_connection=pyexasol_connection,
                                             bucketfs_path=bucketfs_path,
                                             language_alias=slc_builder.language_alias)
        deployer.run(container_file=export_slc, alter_system=True, allow_override=True)
