import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Any, Generator

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
    """
    This subclass is a workaround for problems with poetry export during the SLC build in this project.
    We investigate the issue further in: https://github.com/exasol/advanced-analytics-framework/issues/253
    We disabled the cwd=str(project_directory) in the subprocess call and this helps.
    However, this can cause issues, if you are not in the project directory while building the SLC.
    """

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
            ["poetry", "export", "--without-hashes", "--without-urls"],
            # cwd=str(project_directory) we got the export running by disabling this line
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
def custom_slc_builder() -> Generator[AAFLanguageContainerBuilder | Any, Any, None]:
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with AAFLanguageContainerBuilder(SLC_NAME) as builder:
        builder.prepare_flavor(project_directory)
        yield builder
