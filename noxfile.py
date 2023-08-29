from pathlib import Path

import nox
from nox import Session

ROOT_DIR = Path(__file__).parent
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
def install_lua_environment(session: Session):
    install_script = SCRIPTS_DIRECTORY / "install_lua_environment.sh"
    session.run(str(install_script))


@nox.session(python=False)
def run_lua_unit_tests(session: Session):
    lua_tests_script = SCRIPTS_DIRECTORY / "lua_tests.sh"
    _run_in_dev_env_call(session, str(lua_tests_script))


@nox.session(python=False)
def run_python_unit_tests(session: Session):
    unit_test_directory = TEST_DIRECTORY / "unit_tests"
    _run_in_dev_env_poetry_call(session, "pytest", str(unit_test_directory))


@nox.session(python=False)
def run_python_integration_tests_without_db(session: Session):
    integration_test_directory = INTEGRATION_TEST_DIRECTORY / "without_db"
    _run_in_dev_env_poetry_call(session, "pytest", str(integration_test_directory))


@nox.session(python=False)
def run_python_integration_tests_with_db(session: Session):
    integration_test_directory = INTEGRATION_TEST_DIRECTORY / "with_db"
    _run_in_dev_env_poetry_call(session, "pytest", str(integration_test_directory))


@nox.session(python=False)
def run_python_test(session: Session):
    _run_in_dev_env_poetry_call(session, "pytest", *session.posargs)


@nox.session(python=False)
def start_integration_test_environment(session: Session):
    script_path = SCRIPTS_DIRECTORY / "start_integration_test_environment.sh"
    _run_in_dev_env_call(session, str(script_path))


@nox.session(python=False)
def build_language_container(session: Session):
    script_path = ROOT_DIR / "build_language_container.sh"
    session.run(str(script_path))
