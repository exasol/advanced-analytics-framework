import pytest

from exasol_advanced_analytics_framework.slc import custom_slc_builder
from exasol.python_extension_common.deployment.language_container_builder import (
    find_path_backwards,
    LanguageContainerBuilder,
)


@pytest.fixture(scope="session")
def slc_builder() -> LanguageContainerBuilder:
    """
    Overrides default definition from pytest-exasol-slc.
    """
    test_package = find_path_backwards("tests/test_package", __file__)
    with custom_slc_builder() as builder:
        builder.prepare_flavor(test_package)
        yield builder
