from contextlib import contextmanager
from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards
)

# new comment to check Docker image

# Prefering other name than slc_factory()
# flavor path is predefined by exasol-python-extension-common
@contextmanager
def custom_slc_builder() -> LanguageContainerBuilder:
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with LanguageContainerBuilder(
        container_name="exasol_advanced_analytics_framework_container",
        language_alias="PYTHON3_AAF",
    ) as builder:
        builder.prepare_flavor(project_directory)
        yield builder
