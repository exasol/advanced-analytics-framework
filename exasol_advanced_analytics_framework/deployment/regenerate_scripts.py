import logging
from exasol_advanced_analytics_framework.deployment.bundle_lua_scripts import \
    BundleLuaScripts


def generate_scripts():
    """
    Generate the  Lua sql statement of the Query-Loop from scratch and save it.
    """
    logging.basicConfig(
        format='%(asctime)s - %(module)s  - %(message)s',
        level=logging.DEBUG)

    BundleLuaScripts.save_statement()


if __name__ == "__main__":
    generate_scripts()
