import pyexasol
import pytest
from tests.utils.parameters import db_params


def revert_language_settings(func):
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
