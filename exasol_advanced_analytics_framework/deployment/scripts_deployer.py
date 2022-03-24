import pyexasol
from importlib_resources import files
from jinja2 import Environment, PackageLoader, select_autoescape


class ScriptsDeployer:
    def __init__(self, language_alias: str, schema: str,
                 pyexasol_conn: pyexasol.ExaConnection):
        self._language_alias = language_alias
        self._schema = schema
        self._pyexasol_conn = pyexasol_conn

        self._base_dir = "advanced_analytics_framework"
        self._resource_dir = "resource"
        self._source_dir = files("advanced_analytics_framework.interface")
        self._templates_for_udf_calls = {
            "create_event_handler_udf_call.py": "create_event_handler.jinja.sql"
        }

    def _open_schema(self):
        queries = ["CREATE SCHEMA IF NOT EXISTS {schema_name}",
                   "OPEN SCHEMA {schema_name}"]
        for query in queries:
            self._pyexasol_conn.execute(query.format(schema_name=self._schema))

    def _deploy_udf_scripts(self):
        for udf_call_src, template_src in self._templates_for_udf_calls.items():
            udf_content = self._source_dir.joinpath(udf_call_src).read_text()
            env = Environment(
                loader=PackageLoader(self._base_dir, self._resource_dir),
                autoescape=select_autoescape()
            )
            template = env.get_template(template_src)
            udf_query = template.render(
                script_content=udf_content, language_alias=self._language_alias)
            self._pyexasol_conn.execute(udf_query)

    def deploy_scripts(self):
        self._open_schema()
        self._deploy_udf_scripts()

    @classmethod
    def run(cls, dsn: str, user: str, password: str,
            schema: str, language_alias: str):

        pyexasol_conn = pyexasol.connect(dsn=dsn, user=user, password=password)
        scripts_deployer = cls(language_alias, schema, pyexasol_conn)
        scripts_deployer.deploy_scripts()


