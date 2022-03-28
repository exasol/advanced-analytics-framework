import logging
import click
from exasol_advanced_analytics_framework.deployment. \
    language_container_deployer_cli import language_container_deployer_main
from exasol_advanced_analytics_framework.deployment. \
    scripts_deployer_cli import scripts_deployer_main


@click.group()
def main():
    pass


main.add_command(scripts_deployer_main)
main.add_command(language_container_deployer_main)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(module)s  - %(message)s',
        level=logging.DEBUG)

    main()
