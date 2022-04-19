import pyexasol
import logging
from jinja2 import Environment, PackageLoader, select_autoescape
from exasol_advanced_analytics_framework.deployment import constants
from exasol_advanced_analytics_framework.deployment.bundle_lua_scripts import \
    BundleLuaScripts

logger = logging.getLogger(__name__)


class ScriptsDeployer:
    def __init__(self, language_alias: str, schema: str,
                 pyexasol_conn: pyexasol.ExaConnection):
        self._language_alias = language_alias
        self._schema = schema
        self._pyexasol_conn = pyexasol_conn
        logger.debug(f"Init {ScriptsDeployer.__name__}.")

    def _open_schema(self) -> None:
        queries = ["CREATE SCHEMA IF NOT EXISTS {schema_name}",
                   "OPEN SCHEMA {schema_name}"]
        for query in queries:
            self._pyexasol_conn.execute(query.format(schema_name=self._schema))
        logger.debug(f"Schema {self._schema} is opened.")

    def _deploy_udf_scripts(self) -> None:
        for udf_call_src, template_src in constants.UDF_CALL_TEMPLATES.items():
            udf_content_path = constants.SOURCE_DIR.joinpath(udf_call_src)
            udf_content = udf_content_path.read_text()
            env = Environment(
                loader=PackageLoader(
                    constants.BASE_DIR, constants.TEMPLATES_DIR),
                autoescape=select_autoescape()
            )
            template = env.get_template(template_src)
            udf_query = template.render(
                script_content=udf_content,
                language_alias=self._language_alias)
            self._pyexasol_conn.execute(udf_query)
            logger.debug(f"UDF statement of the template "
                         f"{template_src} is executed.")

    def _deploy_lua_scripts(self) -> None:
        with open(constants.LUA_SCRIPT_OUTPUT, "r") as file:
            lua_query = file.read()
            self._pyexasol_conn.execute(lua_query)
            logger.debug(f"The Lua statement of the template "
                         f"{constants.LUA_SCRIPT_TEMPLATE} is executed.")

    def deploy_scripts(self) -> None:
        self._open_schema()
        self._deploy_udf_scripts()
        self._deploy_lua_scripts()
        logger.debug(f"Scripts are deployed.")

    @classmethod
    def run(cls, dsn: str, user: str, password: str,
            schema: str, language_alias: str, develop: bool):

        if develop:
            BundleLuaScripts.save_statement()

        pyexasol_conn = pyexasol.connect(dsn=dsn, user=user, password=password)
        scripts_deployer = cls(language_alias, schema, pyexasol_conn)
        scripts_deployer.deploy_scripts()
