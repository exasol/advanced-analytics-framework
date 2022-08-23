import typing
from io import StringIO

from jinja2 import Template

from exasol_advanced_analytics_framework.deployment.lua_script_bundle import LuaScriptBundle


class ExasolLuaScriptGenerator:
    def __init__(self, lua_script_bundle: LuaScriptBundle,
                 jinja_template: Template,
                 **kwargs):
        self._jinja_template = jinja_template
        self._lua_script_bundle = lua_script_bundle
        self._kwargs = kwargs

    def generate_script(self, output_buffer: typing.IO) -> None:
        bundle_output_buffer = StringIO()
        self._lua_script_bundle.bundle_lua_scripts(bundle_output_buffer)
        output = self._jinja_template.render(bundled_script=bundle_output_buffer.getvalue(), **self._kwargs)
        output_buffer.write(output)