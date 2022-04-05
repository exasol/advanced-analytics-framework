import os
import subprocess
import tempfile
import logging
import importlib_resources

logger = logging.getLogger(__name__)


class BundleLuaScripts:
    def __init__(self, tmp_dir: str, lua_bundled_file_path: str ):
        self.lua_bundled_file_path = lua_bundled_file_path
        self.tmp_dir = tmp_dir

        self.base_dir = importlib_resources.files(
            "exasol_advanced_analytics_framework")
        self.lua_src_dir = self.base_dir.joinpath("lua/src")
        self.lua_source_list = [
            "event_loop_main.lua",
            "event_loop.lua"]
        self.modules = [
            "event_loop_main.lua",
            "event_loop",
            "exaerror",
            "message_expander"]

    def copy_lua_source_files(self):
        for lua_src_file in self.lua_source_list:
            src_data = self.lua_src_dir.joinpath(lua_src_file).read_text()
            with open(os.path.join(self.tmp_dir , lua_src_file), "w") as file:
                file.write(src_data)
                logger.debug(f"Copy {lua_src_file} to {self.tmp_dir }")

    def bundle_lua_scripts(self):
        bash_command = \
            "cd {tmp_dir} && amalg.lua -o {out_path} -s {modules}".format(
                tmp_dir=self.tmp_dir,
                out_path=self.lua_bundled_file_path,
                modules=" ".join(self.modules))

        subprocess.check_call(bash_command, shell=True)
        logger.debug(f"Lua scripts are bundled "
                     f"into {self.lua_bundled_file_path}")

    @classmethod
    def get_content(cls) -> str:
        with tempfile.TemporaryDirectory() as tmp_dir:
            lua_bundled_file_path = os.path.join(tmp_dir, "bundle_final.lua")
            bundler = cls(tmp_dir, lua_bundled_file_path)

            bundler.copy_lua_source_files()
            bundler.bundle_lua_scripts()
            with open(os.path.join(
                    tmp_dir, lua_bundled_file_path), "r") as file:
                lua_bundled_data = file.read()
        return lua_bundled_data
