import os
import tempfile
from io import StringIO

from exasol_advanced_analytics_framework.deployment.aaf_exasol_lua_script_generator import \
    get_aaf_query_loop_lua_script_generator
from exasol_advanced_analytics_framework.deployment.lua_script_bundle import \
    LuaScriptBundle


def test_get_aaf_query_loop_lua_script_generator():
    generator = get_aaf_query_loop_lua_script_generator()
    output_buffer = StringIO()
    generator.generate_script(output_buffer)
    script = output_buffer.getvalue()

    assert script.startswith("""CREATE OR REPLACE LUA SCRIPT "AAF_RUN_QUERY_HANDLER"(json_str) RETURNS TABLE AS""") and \
           "function query_handler_runner_main" in script and \
           "query_handler_runner_main(json_str, exa)"
