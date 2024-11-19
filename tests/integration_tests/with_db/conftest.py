import pytest
from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards,
)

from exasol.analytics.query_handler.deployment.slc import (
    LANGUAGE_ALIAS,
    custom_slc_builder,
)


@pytest.fixture(scope="session")
def language_alias():
    return LANGUAGE_ALIAS


@pytest.fixture(scope="session")
def slc_builder(use_onprem, use_saas) -> LanguageContainerBuilder:
    """
    Overrides default definition from pytest-exasol-slc.

    This slc_builder uses the default builder of the AAF, defined in
    exasol.analytics.slc.custom_slc_builder and adds
    another wheel and its pip requirements on top.

    In result the SLC will contain AAF and the subproject from directory
    tests/test_package.
    """
    if use_saas or use_onprem:
        test_package = find_path_backwards("tests/test_package", __file__)
        with custom_slc_builder() as builder:
            builder.prepare_flavor(test_package)
            yield builder
    else:
        yield None
