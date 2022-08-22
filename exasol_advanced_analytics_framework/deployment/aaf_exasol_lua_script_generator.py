import importlib_resources

from exasol_advanced_analytics_framework.deployment import constants
from exasol_advanced_analytics_framework.deployment.exasol_lua_script_generator import ExasolLuaScriptGenerator
from exasol_advanced_analytics_framework.deployment.jinja_template_location import JinjaTemplateLocation
from exasol_advanced_analytics_framework.deployment.lua_script_bundle import LuaScriptBundle, logger


def get_aaf_query_loop_lua_script_generator() -> ExasolLuaScriptGenerator:
    base_dir = importlib_resources.files(
        constants.BASE_DIR)
    lua_src_dir = base_dir.joinpath("lua/src")
    lua_source_files = [
        lua_src_dir.joinpath("query_loop_main.lua"),
        lua_src_dir.joinpath("query_loop.lua")
    ]
    lua_main_file = lua_src_dir.joinpath("query_loop_main.lua")
    lua_modules = [
        "query_loop",
        "exaerror",
        "message_expander"
    ]
    jinja_template_location = JinjaTemplateLocation(
        package_name=constants.BASE_DIR,
        package_path=constants.TEMPLATES_DIR,
        template_file_name=constants.LUA_SCRIPT_TEMPLATE)
    generator = ExasolLuaScriptGenerator(
        LuaScriptBundle(lua_main_file=lua_main_file,
                        lua_modules=lua_modules,
                        lua_source_files=lua_source_files),
        jinja_template_location.get_template()
    )
    return generator


def save_aaf_query_loop_lua_script() -> None:
    generator = get_aaf_query_loop_lua_script_generator()
    with open(constants.LUA_SCRIPT_OUTPUT, "w") as file:
        generator.generate_script(file)
        logger.debug(f"The Lua bundled statement saved.")
