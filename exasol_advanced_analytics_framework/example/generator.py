import json
import importlib.resources
from jinja2 import Template, Environment, PackageLoader, select_autoescape
from pathlib import Path
from exasol_advanced_analytics_framework.deployment import constants
from exasol_advanced_analytics_framework.deployment.jinja_template_location import JinjaTemplateLocation

PACKAGE_PATH = "example"

QUERY_HANDLER_SCRIPT = {
    "query_handler": {
        "factory_class": {
            "module": "xyz",
            "name": "ExampleQueryHandlerFactory",
        },
        "parameters": "bla-bla",
        "udf": {
            "schema": "MY_SCHEMA",
            "name": "MY_QUERY_HANDLER_UDF",
        },
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": "BFS_CON",
            "directory": "temp",
        },
        "schema_name": "TEMP_SCHEMA",
    },
}

def jinja_env():
    return Environment(
        loader=PackageLoader(
            package_name=constants.BASE_DIR,
            package_path=PACKAGE_PATH),
        autoescape=select_autoescape()
    )


def generate():
    env = jinja_env()
    python_code = importlib.resources.read_text(
        f"{constants.BASE_DIR}.{PACKAGE_PATH}",
        "query_handler.py",
    )
    json_code = json.dumps(QUERY_HANDLER_SCRIPT, indent=4)
    template = env.get_template("sql.jinja")
    return template.render(
        python_code=python_code,
        json_code=json_code,
    )
