import contextlib
import pyexasol   # type: ignore

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
