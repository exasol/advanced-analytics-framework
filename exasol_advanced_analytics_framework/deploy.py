import logging
import click
from exasol_advanced_analytics_framework.slc import (
    SLC_FILE_NAME,
    SLC_URL_FORMATTER,
)
from exasol_advanced_analytics_framework.deployment import (
    scripts_deployer_cli,
    language_container_deployer_cli,
)
from exasol.python_extension_common.deployment.language_container_deployer_cli import (
    language_container_deployer_main,
    slc_parameter_formatters,
    CustomizableParameters,
)


@click.group()
def main():
    pass


slc_parameter_formatters.set_formatter(CustomizableParameters.container_url, SLC_URL_FORMATTER)
slc_parameter_formatters.set_formatter(CustomizableParameters.container_name, SLC_FILE_NAME)

main.add_command(scripts_deployer_cli.scripts_deployer_main)
main.add_command(language_container_deployer_cli.language_container_deployer_main)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s - %(module)s  - %(message)s',
        level=logging.DEBUG)

    main()
