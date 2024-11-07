import pathlib
from importlib_resources import files


BASE_PACKAGE = "exasol.analytics"
BASE_DIR = BASE_PACKAGE.replace(".", "/")
# BASE_DIR = "exasol/analytics"
# BASE_PACKAGE = BASE_DIR.replace("/", ".")
TEMPLATES_DIR = pathlib.Path("resources", "templates")
OUTPUTS_DIR = pathlib.Path("resources", "outputs")
# SOURCE_DIR = files(f"{BASE_PACKAGE}.query_handler.udf")
SOURCE_DIR = files(f"{BASE_PACKAGE}.query_handler.udf.runner")

UDF_CALL_TEMPLATES = {
#    "query_handler_runner_udf_call.py": "create_query_handler.jinja.sql"
    "call_udf.py": "create_query_handler.jinja.sql"
}
LUA_SCRIPT_TEMPLATE = "create_query_loop.jinja.sql"
LUA_SCRIPT_OUTPUT = pathlib.Path(BASE_DIR, OUTPUTS_DIR, "create_query_loop.sql")


# if __name__ == "__main__":
#     print(f'{SOURCE_DIR}')
#     for udf_call_src, template_src in UDF_CALL_TEMPLATES.items():
#         f = SOURCE_DIR / udf_call_src
#         print(f'{f} {template_src}')
