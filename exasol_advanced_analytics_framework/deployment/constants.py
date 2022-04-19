import os
from importlib_resources import files

BASE_DIR = "exasol_advanced_analytics_framework"
TEMPLATES_DIR = os.path.join("resource", "templates")
OUTPUTS_DIR = os.path.join("resource", "outputs")
SOURCE_DIR = files(f"{BASE_DIR}.interface")

UDF_CALL_TEMPLATES = {
    "create_event_handler_udf_call.py": "create_event_handler.jinja.sql"
}
LUA_SCRIPT_TEMPLATE = "create_event_loop.jinja.sql"
LUA_SCRIPT_OUTPUT = os.path.join(BASE_DIR, OUTPUTS_DIR, "create_event_loop.sql")
