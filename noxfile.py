import json
import os
import re
from datetime import datetime
from pathlib import Path

import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *
from nox import Session

from exasol.analytics.query_handler.deployment.slc import custom_slc_builder
from noxconfig import ROOT_DIR

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["fix"]

SCRIPTS_DIRECTORY = ROOT_DIR / "scripts"
RUN_IN_DEV_SCRIPT = SCRIPTS_DIRECTORY / "run_in_dev_env.sh"
RUN_IN_DEV_SCRIPT_STR = str(RUN_IN_DEV_SCRIPT)
TEST_DIRECTORY = ROOT_DIR / "test"
INTEGRATION_TEST_DIRECTORY = TEST_DIRECTORY / "integration"

nox.options.sessions = []


def _run_in_dev_env_poetry_call(session: Session, *args: str | os.PathLike[str]):
    session.run(RUN_IN_DEV_SCRIPT_STR, "poetry", "run", *args)


def _run_in_dev_env_call(session: Session, *args: str | os.PathLike[str]):
    session.run(RUN_IN_DEV_SCRIPT_STR, *args)


@nox.session(name="devenv:run", python=False)
def run_in_dev_env(session: Session):
    _run_in_dev_env_call(session, *session.posargs)


@nox.session(name="devenv:poetry", python=False)
def run_in_dev_env_poetry(session: Session):
    """
    This Nox session calls poetry run in devenv and is designed to be used
    interactively by human developers on-demand.
    """
    _run_in_dev_env_poetry_call(session, *session.posargs)


@nox.session(name="devenv:pytest", python=False)
def run_python_test(session: Session):
    """
    This Nox session is currently unused.
    """
    _run_in_dev_env_poetry_call(session, "pytest", *session.posargs)


@nox.session(name="devenv:pytest", python=False)
def run_python_unit_tests(session: Session):
    """
    This Nox session is currently unused, as unit tests don't need devenv
    and can be executed with PTB standard Nox session test:unit.
    """
    dir = TEST_DIRECTORY / "unit"
    _run_in_dev_env_poetry_call(session, "pytest", dir)


@nox.session(name="itests:no-db", python=False)
def run_python_integration_tests_no_db(session: Session):
    """
    This Nox session is currently unused, see Developer Guide on why to
    prefer session devenv:pytest.
    """
    dir = INTEGRATION_TEST_DIRECTORY / "no_db"
    _run_in_dev_env_poetry_call(session, "pytest", dir, *session.posargs)


@nox.session(name="devenv:install", python=False)
def install_dev_env(session: Session):
    script = SCRIPTS_DIRECTORY / "install_development_environment.sh"
    session.run(script)


@nox.session(name="lua:amalgate", python=False)
def amalgate_lua_scripts(session: Session):
    script = (
        ROOT_DIR
        / "exasol"
        / "analytics"
        / "query_handler"
        / "deployment"
        / "regenerate_scripts.py"
    )
    _run_in_dev_env_poetry_call(session, "python", script)


@nox.session(name="lua:unit-tests", python=False)
def run_lua_unit_tests(session: Session):
    script = SCRIPTS_DIRECTORY / "lua_tests.sh"
    _run_in_dev_env_call(session, script)


@nox.session(name="matrix:no-db", python=False)
def write_github_integration_tests_no_db_matrix(session: Session):
    dir = INTEGRATION_TEST_DIRECTORY / "no_db"

    def entry(file: Path):
        short = (
            re.sub(r"_communication|test_|\.py", "", str(file.relative_to(dir)))
            .replace("/", " \u2022 ")
            .replace("_", " ")
            .title()
        )
        return {"short": short, "path": str(file.relative_to(ROOT_DIR))}

    globbed = dir.rglob("test_*.py")
    files = [entry(file) for file in globbed]
    json_str = json.dumps(files)
    print(f"files={json_str}")



@nox.session(name="slc:build", python=False)
def build_language_container(session: Session):
    export_path = ROOT_DIR / ".slc"
    with custom_slc_builder() as builder:
        builder.export(export_path)


@nox.session(name="itests:with-db", python=False)
def run_python_integration_tests_with_db(session: Session):
    dir = INTEGRATION_TEST_DIRECTORY / "with_db"
    _run_in_dev_env_poetry_call(session, "pytest", dir, *session.posargs)
