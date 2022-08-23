import logging

from exasol_advanced_analytics_framework.deployment.aaf_exasol_lua_script_generator import \
    save_aaf_query_loop_lua_script
from exasol_advanced_analytics_framework.deployment.lua_script_bundle import \
    LuaScriptBundle


def generate_scripts():
    """
    Generate the  Lua sql statement of the Query-Loop from scratch and save it.
    """
    logging.basicConfig(
        format='%(asctime)s - %(module)s  - %(message)s',
        level=logging.DEBUG)

    save_aaf_query_loop_lua_script()


if __name__ == "__main__":
    generate_scripts()
