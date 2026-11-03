from pathlib import Path

from exasol.toolbox.config import BaseConfig

ROOT_DIR = Path(__file__).parent


PROJECT_CONFIG = BaseConfig(
    root_path=ROOT_DIR,
    project_name="analytics",
    add_to_excluded_python_paths=(".conda_env", ".luarocks"),
)
