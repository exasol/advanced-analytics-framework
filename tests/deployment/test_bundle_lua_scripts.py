import os
import tempfile

from exasol_advanced_analytics_framework.deployment.bundle_lua_scripts import \
    BundleLuaScripts


def test_bundle_lua_scripts():
    lua_bundled_content = BundleLuaScripts.get_content()
    assert lua_bundled_content

    with tempfile.TemporaryDirectory() as tmp_dir:
        lua_bundled_file_path = os.path.join(tmp_dir, "bundle_final.lua")
        bundler = BundleLuaScripts(tmp_dir, lua_bundled_file_path)

        bundler.copy_lua_source_files()
        bundler.bundle_lua_scripts()
        with open(os.path.join(
                tmp_dir, lua_bundled_file_path), "r") as file:
            lua_bundled_data = file.read()
        assert lua_bundled_data == lua_bundled_content

