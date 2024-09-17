import contextlib
import pyexasol   # type: ignore
import pytest
from tests.utils.parameters import db_params

# obsolete, only used by
# tests/deployment/test__deployer_cli.py
# tests/deployment/test__deployer.py
def revert_language_settings_old(func):
    def wrapper(language_alias, schema, db_conn,
                container_path, language_settings):
        try:
            return func(language_alias, schema, db_conn,
                        container_path, language_settings)
        except Exception as exc:
            print("Exception occurred while running the test: %s" % exc)
            raise pytest.fail(exc)
        finally:
            print("Revert language settings")
            db_conn_revert = pyexasol.connect(
                dsn=db_params.address(),
                user=db_params.user,
                password=db_params.password)
            db_conn_revert.execute(f"ALTER SYSTEM SET SCRIPT_LANGUAGES="
                                   f"'{language_settings[0][0]}';")
            db_conn_revert.execute(f"ALTER SESSION SET SCRIPT_LANGUAGES="
                                   f"'{language_settings[0][1]}';")

    return wrapper


@contextlib.contextmanager
def revert_language_settings(connection: pyexasol.ExaConnection):
    query = f"""
        SELECT "SYSTEM_VALUE", "SESSION_VALUE"
        FROM SYS.EXA_PARAMETERS
        WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'"""
    language_settings = connection.execute(query).fetchall()[0]
    try:
        yield
    finally:
        connection.execute(f"ALTER SYSTEM SET SCRIPT_LANGUAGES='{language_settings[0]}';")
        connection.execute(f"ALTER SESSION SET SCRIPT_LANGUAGES='{language_settings[1]}';")
