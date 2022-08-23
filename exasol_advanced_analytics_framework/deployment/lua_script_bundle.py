import logging
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, IO, Union

from importlib_resources.abc import Traversable

logger = logging.getLogger(__name__)

PathLike = Union[Path, Traversable]


class LuaScriptBundle:
    def __init__(self,
                 lua_main_file: PathLike,
                 lua_source_files: List[PathLike],
                 lua_modules: List[str]):
        self.lua_main_file = lua_main_file
        self.lua_modules = lua_modules
        self.lua_source_files = lua_source_files

    def bundle_lua_scripts(self, output_buffer: IO):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            self.copy_lua_source_files(tmp_dir)
            self.run_lua_amlg(tmp_dir, output_buffer)

    def copy_lua_source_files(self, tmp_dir: Path):
        for src in self.lua_source_files + [self.lua_main_file]:
            dst = tmp_dir / src.name
            logger.debug(f"Copy {src} to {tmp_dir}")
            shutil.copy(src, dst)

    def run_lua_amlg(self, tmp_dir: Path, output_buffer: IO):
        output_file = tmp_dir / f"bundle_{time.time()}.lua"
        bash_command = \
            "amalg.lua -o {out_path} -s {main_file} {modules}".format(
                tmp_dir=tmp_dir,
                out_path=output_file,
                main_file=self.lua_main_file.name,
                modules=" ".join(self.lua_modules))
        subprocess.check_call(bash_command, shell=True, cwd=tmp_dir)
        with output_file.open() as f:
            shutil.copyfileobj(f, output_buffer)
        logger.debug(f"Lua scripts are bundled")
