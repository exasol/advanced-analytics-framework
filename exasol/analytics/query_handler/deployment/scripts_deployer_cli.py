import click
from exasol.analytics.query_handler.deployment import utils
from exasol.analytics.query_handler.deployment.scripts_deployer import     ScriptsDeployer
from exasol.analytics.query_handler.deployment.slc import LANGUAGE_ALIAS

@click.command(name="scripts")
@click.option('--dsn', type=str, required=True)
@click.option('--user', type=str, required=True)
@click.option('--pass', 'pwd', type=str)
@click.option('--schema', type=str, required=True)
@click.option('--language-alias', type=str, default=LANGUAGE_ALIAS)
@click.option('--develop', type=bool, is_flag=True)
def scripts_deployer_main(
        dsn: str, user: str, pwd: str, schema: str,
        language_alias: str, develop: bool):
    password = utils.get_password(
        pwd, user, utils.DB_PASSWORD_ENVIRONMENT_VARIABLE, "DB Password")
    ScriptsDeployer.run(
        dsn=dsn,
        user=user,
        password=password,
        schema=schema,
        language_alias=language_alias,
        develop=develop,
    )


if __name__ == '__main__':
    import logging
    logging.basicConfig(
        format='%(asctime)s - %(module)s  - %(message)s',
        level=logging.DEBUG)

    scripts_deployer_main()
