import pyexasol
from importlib_resources import files
from jinja2 import Environment, PackageLoader, select_autoescape
import logging
logger = logging.getLogger(__name__)


class ScriptsDeployer:
    def __init__(self, language_alias: str, schema: str,
                 pyexasol_conn: pyexasol.ExaConnection):
        self._language_alias = language_alias
        self._schema = schema
        self._pyexasol_conn = pyexasol_conn

        self._base_dir = "exasol_advanced_analytics_framework"
        self._resource_dir = "resource"
        self._source_dir = files(f"{self._base_dir}.interface")
        self._templates_for_udf_calls = {
            "create_event_handler_udf_call.py": "create_event_handler.jinja.sql"
        }
        logger.debug(f"Init {ScriptsDeployer.__name__}.")

    def _open_schema(self):
        queries = ["CREATE SCHEMA IF NOT EXISTS {schema_name}",
                   "OPEN SCHEMA {schema_name}"]
        for query in queries:
            self._pyexasol_conn.execute(query.format(schema_name=self._schema))
        logger.debug(f"Schema {self._schema} is opened.")

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
            logger.debug(f"UDF statement of the template "
                         f"{template_src} is executed.")

    def deploy_scripts(self):
        self._open_schema()
        self._deploy_udf_scripts()
        logger.debug(f"Scripts are deployed.")

    @classmethod
    def run(cls, dsn: str, user: str, password: str,
            schema: str, language_alias: str):

        pyexasol_conn = pyexasol.connect(dsn=dsn, user=user, password=password)
        scripts_deployer = cls(language_alias, schema, pyexasol_conn)
        scripts_deployer.deploy_scripts()


