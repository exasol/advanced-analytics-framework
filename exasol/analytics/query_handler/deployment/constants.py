import pathlib
from importlib_resources import files


BASE_PACKAGE = "exasol.analytics"
BASE_DIR = BASE_PACKAGE.replace(".", "/")
TEMPLATES_DIR = pathlib.Path("resources", "templates")
OUTPUTS_DIR = pathlib.Path("resources", "outputs")
SOURCE_DIR = files(f"{BASE_PACKAGE}.query_handler.udf.runner")

UDF_CALL_TEMPLATES = {
    "call_udf.py": "create_query_handler.jinja.sql"
}
LUA_SCRIPT_TEMPLATE = "create_query_loop.jinja.sql"
LUA_SCRIPT_OUTPUT = pathlib.Path(BASE_DIR, OUTPUTS_DIR, "create_query_loop.sql")
