from contextlib import contextmanager

from exasol.python_extension_common.deployment.language_container_builder import ( LanguageContainerBuilder, find_path_backwards, ) # type: ignore[import-untyped]

LANGUAGE_ALIAS = "PYTHON3_AAF"
SLC_NAME = "exasol_advanced_analytics_framework_container"
SLC_FILE_NAME = SLC_NAME + "_release.tar.gz"
SLC_URL_FORMATTER = (
    "https://github.com/exasol/advanced-analytics-framework/releases/download/{version}/"
    + SLC_FILE_NAME
)


@contextmanager
def custom_slc_builder() -> LanguageContainerBuilder:
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with LanguageContainerBuilder(SLC_NAME) as builder:
        builder.prepare_flavor(project_directory)
        yield builder
