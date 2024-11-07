import subprocess
from pathlib import Path

from exasol.analytics.deployment.lua_script_bundle import LuaScriptBundle


def test(tmp_path):
    resource_dir = Path(__file__).parent / "resources" / "lua_bundle"
    bundle = LuaScriptBundle(lua_main_file=resource_dir / "main.lua",
                             lua_source_files=[resource_dir / "test_module_1.lua"],
                             lua_modules=["test_module_1"])
    bundle_file_name = "bundle.lua"
    bundle_lua = tmp_path / bundle_file_name
    with bundle_lua.open("w") as file:
        bundle.bundle_lua_scripts(file)
    output = subprocess.check_output(["lua", bundle_file_name], cwd=tmp_path)
    output_decode = output.decode("utf-8").strip()
    assert output_decode == "TEST_OUTPUT"