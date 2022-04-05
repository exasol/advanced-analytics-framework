from typing import List

deployed_script_list = [
    "AAF_EVENT_HANDLER_UDF",
    "AAF_EVENT_LOOP"
]


class DBQueries:
    @staticmethod
    def get_all_scripts(db_conn, schema_name) -> List[int]:
        query_all_scripts = \
            f"""
                SELECT SCRIPT_NAME 
                FROM EXA_ALL_SCRIPTS
                WHERE SCRIPT_SCHEMA = '{schema_name.upper()}'
            """
        all_scripts = db_conn.execute(query_all_scripts).fetchall()
        return list(map(lambda x: x[0], all_scripts))

    @staticmethod
    def check_all_scripts_deployed(db_conn, schema_name) -> bool:
        all_scripts = DBQueries.get_all_scripts(
            db_conn, schema_name)
        return all(script in all_scripts for script in deployed_script_list)

    @staticmethod
    def get_language_settings(db_conn) -> List:
        query = f"""
            SELECT "SYSTEM_VALUE", "SESSION_VALUE" 
            FROM SYS.EXA_PARAMETERS 
            WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'"""
        return db_conn.execute(query).fetchall()

    @staticmethod
    def get_language_settings_from(db_conn, alter_type):
        result = DBQueries.get_language_settings(db_conn)
        return result[0][0] if alter_type.upper() == "SYSTEM" else result[0][1]

    @staticmethod
    def set_language_settings_to(db_conn, alter_type, language_settings):
        db_conn.execute(
            f"""ALTER {alter_type.upper()} SET SCRIPT_LANGUAGES=
            '{language_settings}'""")
