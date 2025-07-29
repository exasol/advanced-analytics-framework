from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT_DIR = Path(__file__).parent


@dataclass(frozen=True)
class Config:
    root: Path = ROOT_DIR
    doc: Path = ROOT_DIR / "doc"
    version_file: Path = ROOT_DIR / "version.py"
    source: Path = Path("exasol/analytics")
    path_filters: Iterable[str] = (
        "dist",
        ".eggs",
        "venv",
        ".conda_env",
        ".poetry",
        ".luarocks",
    )
    python_versions = ["3.10"]


PROJECT_CONFIG = Config()
