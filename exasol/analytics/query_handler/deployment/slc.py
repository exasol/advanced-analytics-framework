import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards,
)

LANGUAGE_ALIAS = "PYTHON3_AAF"
SLC_NAME = "exasol_advanced_analytics_framework_container"
SLC_FILE_NAME = SLC_NAME + "_release.tar.gz"
SLC_URL_FORMATTER = (
    "https://github.com/exasol/advanced-analytics-framework/releases/download/{version}/"
    + SLC_FILE_NAME
)


class AAFLanguageContainerBuilder(LanguageContainerBuilder):
    def _add_requirements_to_flavor(
        self,
        project_directory: str | Path,
        requirement_filter: Callable[[str], bool] | None,
    ):
        """
        Adds project's requirements to the requirements.txt file. Creates this file
        if it doesn't exist.
        """
        assert self._root_path is not None
        requirements_bytes = subprocess.check_output(
            ["poetry", "export", "--without-hashes", "--without-urls"]
        )
        requirements = requirements_bytes.decode("UTF-8")
        if requirement_filter is not None:
            requirements = "\n".join(
                filter(requirement_filter, requirements.splitlines())
            )
        # Make sure the content ends with a new line, so that other requirements can be
        # added at the end of it.
        if not requirements.endswith("\n"):
            requirements += "\n"
        with self.requirements_file.open(mode="a") as f:
            return f.write(requirements)


@contextmanager
def custom_slc_builder() -> LanguageContainerBuilder:
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with AAFLanguageContainerBuilder(SLC_NAME) as builder:
        builder.prepare_flavor(project_directory)
        yield builder
