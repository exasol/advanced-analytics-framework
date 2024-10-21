import json
import os
from pathlib import Path
from exasol_advanced_analytics_framework.slc import custom_slc_builder
from scripts.document_updater import update_examples
from datetime import datetime

import nox
from nox import Session

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *
from noxconfig import ROOT_DIR

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["fix"]

SCRIPTS_DIRECTORY = ROOT_DIR / "scripts"
RUN_IN_DEV_SCRIPT = SCRIPTS_DIRECTORY / "run_in_dev_env.sh"
RUN_IN_DEV_SCRIPT_STR = str(RUN_IN_DEV_SCRIPT)
TEST_DIRECTORY = ROOT_DIR / "tests"
INTEGRATION_TEST_DIRECTORY = TEST_DIRECTORY / "integration_tests"

nox.options.sessions = []


def _run_in_dev_env_poetry_call(session: Session, *args: str):
    session.run(RUN_IN_DEV_SCRIPT_STR, "poetry", "run", *args)


def _run_in_dev_env_call(session: Session, *args: str):
    session.run(RUN_IN_DEV_SCRIPT_STR, *args)


@nox.session(python=False)
def run_in_dev_env(session: Session):
    _run_in_dev_env_call(session, *session.posargs)


@nox.session(python=False)
def run_in_dev_env_poetry(session: Session):
    _run_in_dev_env_poetry_call(session, *session.posargs)


@nox.session(python=False)
def run_python_test(session: Session):
    _run_in_dev_env_poetry_call(session, "pytest", *session.posargs)


@nox.session(python=False)
def install_dev_env(session: Session):
    install_script = SCRIPTS_DIRECTORY / "install_development_environment.sh"
    session.run(str(install_script))


@nox.session(python=False)
def amalgate_lua_scripts(session: Session):
    script = ROOT_DIR / "exasol_advanced_analytics_framework" / "deployment" / "regenerate_scripts.py"
    _run_in_dev_env_poetry_call(session, "python", str(script))


@nox.session(python=False)
def run_lua_unit_tests(session: Session):
    lua_tests_script = SCRIPTS_DIRECTORY / "lua_tests.sh"
    _run_in_dev_env_call(session, str(lua_tests_script))


@nox.session(python=False)
def run_python_unit_tests(session: Session):
    unit_test_directory = TEST_DIRECTORY / "unit_tests"
    _run_in_dev_env_poetry_call(session, "pytest", str(unit_test_directory))


def _generate_test_matrix_entry(test_file: Path):
    return {
        "name": str(test_file.name),
        "path": str(test_file)
    }


def _generate_github_integration_tests_without_db_matrix() -> str:
    without_db_test_directory = INTEGRATION_TEST_DIRECTORY / "without_db"
    test_files = without_db_test_directory.rglob("test_*.py")
    output = [_generate_test_matrix_entry(test_file) for test_file in test_files]
    json_str = json.dumps(output)
    return json_str


@nox.session(python=False)
def generate_github_integration_tests_without_db_matrix_json(session: Session):
    json_str = _generate_github_integration_tests_without_db_matrix()
    print(json_str)


@nox.session(python=False)
def write_github_integration_tests_without_db_matrix(session: Session):
    json_str = _generate_github_integration_tests_without_db_matrix()
    github_output_definition = f'matrix={json_str}'
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
            print(github_output_definition, file=fh)
    else:
        print(github_output_definition)


@nox.session(python=False)
def run_python_integration_tests_without_db(session: Session):
    integration_test_directory = INTEGRATION_TEST_DIRECTORY / "without_db"
    _run_in_dev_env_poetry_call(session, "pytest", str(integration_test_directory))


@nox.session(python=False)
def start_integration_test_environment(session: Session):
    script_path = SCRIPTS_DIRECTORY / "start_integration_test_environment.sh"
    _run_in_dev_env_call(session, str(script_path))


@nox.session(python=False)
def build_language_container(session: Session):
    export_path = ROOT_DIR / ".slc"
    with custom_slc_builder() as builder:
        builder.export(export_path)


@nox.session(python=False)
def run_python_integration_tests_with_db(session: Session):
    integration_test_directory = INTEGRATION_TEST_DIRECTORY / "with_db"
    _run_in_dev_env_poetry_call(
        session,
        "pytest",
        str(integration_test_directory),
        *session.posargs,
    )


@nox.session(python=False)
def update_user_guide(session: Session):
    """
    This task updates the examples in the user guide.
    """
    update_examples(Path("doc/user_guide/user_guide.md"))
