import pytest


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
            db_conn.execute(f"ALTER SYSTEM SET SCRIPT_LANGUAGES="
                            f"'{language_settings[0][0]}';")
            db_conn.execute(f"ALTER SESSION SET SCRIPT_LANGUAGES="
                            f"'{language_settings[0][1]}';")

    return wrapper
