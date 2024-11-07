from io import StringIO
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from exasol.analytics.deployment.exasol_lua_script_generator import ExasolLuaScriptGenerator
from exasol.analytics.deployment.lua_script_bundle import LuaScriptBundle


def test():
    resource_dir = Path(__file__).parent / "resources"
    lua_bundle_dir = resource_dir / "lua_bundle"
    bundle = LuaScriptBundle(lua_main_file=lua_bundle_dir / "main.lua",
                             lua_source_files=[lua_bundle_dir / "test_module_1.lua"],
                             lua_modules=["test_module_1"])
    env = Environment(
        loader=FileSystemLoader(resource_dir),
        autoescape=select_autoescape()
    )
    template = env.get_template("create_script.jinja")
    generator = ExasolLuaScriptGenerator(
        lua_script_bundle=bundle,
        jinja_template=template
    )
    output_buffer = StringIO()
    generator.generate_script(output_buffer)
    expected_script_file = resource_dir / "expected_script"
    expected_script = expected_script_file.read_text()
    assert output_buffer.getvalue() == expected_script
