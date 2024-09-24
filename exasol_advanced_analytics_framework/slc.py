from pathlib import Path
from typing import Callable
from contextlib import contextmanager
from exasol.python_extension_common.deployment.language_container_builder import (
    LanguageContainerBuilder,
    find_path_backwards
)

import subprocess

LANGUAGE_ALIAS = "PYTHON3_AAF"
SLC_NAME = "exasol_advanced_analytics_framework_container.tar.gz"
SLC_URL_FORMATTER = "https://github.com/exasol/advanced_analytics_framework/releases/download/{version}/" + SLC_NAME


# Can be removed as soon as new version of PEC is available
# incl. fix of https://github.com/exasol/python-extension-common/issues/60
class IncrementalSlcBuilder(LanguageContainerBuilder):
    """
    This class extends LanguageContainerBuilder to enable adding multiple
    projects in an incremental way. The original LanguageContainerBuilder
    already copies the wheel file, which is incremental by definition.

    But the pip requirements file of subsequent projects will overwrite the
    initial requirements in a non-incremental way.
    """

    def __init__(self, container_name: str, language_alias: str):
        super().__init__(container_name, language_alias)

    @property
    def pip_requirements(self) -> Path:
        return self.flavor_base / "dependencies" / "requirements.txt"

    def _add_requirements_to_flavor(self, project_directory: str | Path,
                                    requirement_filter: Callable[[str], bool] | None):
        """
        Append the project's pip requirements to file requirements.txt.
        """
        assert self._root_path is not None
        before = (self.pip_requirements.read_text()
                  if self.pip_requirements.exists()
                  else "")
        requirements = subprocess.check_output(
            ["poetry", "export", "--without-hashes", "--without-urls"],
            cwd=str(project_directory),
            encoding="UTF-8",
        )
        if requirement_filter is not None:
            requirements = "\n".join(filter(requirement_filter, requirements.splitlines()))
        self.pip_requirements.write_text(before + requirements)


@contextmanager
def custom_slc_builder() -> LanguageContainerBuilder:
    project_directory = find_path_backwards("pyproject.toml", __file__).parent
    with LanguageContainerBuilder(
            "exasol_advanced_analytics_framework_container",
    ) as builder:
        builder.prepare_flavor(project_directory)
        yield builder
