import json
import importlib.resources
from jinja2 import Template, Environment, PackageLoader, BaseLoader, select_autoescape
from pathlib import Path
from exasol_advanced_analytics_framework.deployment import constants
from exasol_advanced_analytics_framework.deployment.jinja_template_location import JinjaTemplateLocation
from typing import Any, Dict

PACKAGE_PATH = "example"

SCRIPT_ARGUMENTS = {
    "query_handler": {
        "factory_class": {
            "module": "example_module",
            "name": "ExampleQueryHandlerFactory",
        },
        "parameter": "bla-bla",
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


def quoted_udf_name(query_handler_script: Dict[str, Any]):
    schema = query_handler_script["udf"]["schema"]
    name = query_handler_script["udf"]["name"]
    return f'"{schema}"."{name}"'


def render_template(template: str, **kwargs) -> str:
    return (
        Environment(loader=BaseLoader)
        .from_string(template)
        .render(**kwargs)
    )


def create_script(script_arguments=SCRIPT_ARGUMENTS):
    script = (
        '--/\n'
        'CREATE OR REPLACE PYTHON3_AAF SET SCRIPT'
        ' "{{ query_handler.udf.schema }}"."{{ query_handler.udf.name }}"(...)\n'
        'EMITS (outputs VARCHAR(2000000)) AS\n'
        '{{ python_code }}\n'
        '/\n'
    )
    python_code = importlib.resources.read_text(
        f"{constants.BASE_DIR}.{PACKAGE_PATH}",
        "query_handler.py",
    )
    return render_template(
        script,
        python_code=python_code,
        **script_arguments,
    )


def execute_script(script_arguments=SCRIPT_ARGUMENTS):
    script = (
        "EXECUTE SCRIPT {{ query_handler.udf.schema }}"
        ".AAF_RUN_QUERY_HANDLER("
        "'{{ json_string }}')"
    )
    json_string = json.dumps(script_arguments, indent=4)
    return render_template(
        script,
        json_string=json_string,
        **script_arguments,
    )
