import click
from exasol_advanced_analytics_framework.deployment import utils
from exasol_advanced_analytics_framework.deployment.scripts_deployer import \
    ScriptsDeployer


@click.command(name="scripts")
@click.option('--dsn', type=str, required=True)
@click.option('--user', type=str, required=True)
@click.option('--pass', 'pwd', type=str)
@click.option('--schema', type=str, required=True)
@click.option('--language-alias', type=str, default="PYTHON_AAF")
def scripts_deployer_main(dsn: str, user: str, pwd: str,
                          schema: str, language_alias: str):
    password = utils.get_password(
        pwd, user, utils.DB_PASSWORD_ENVIRONMENT_VARIABLE, "DB Password")

    ScriptsDeployer.run(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        language_alias=language_alias)


if __name__ == '__main__':
    scripts_deployer_main()
