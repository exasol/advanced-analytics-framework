import textwrap
from inspect import cleandoc

from exasol_data_science_utils_python.schema.udf_name import UDFName


class RetrieveExasolNodeIPAddressUDFDeployer:

    def render_udf_create_statement(self, udf_name: UDFName, language_alias: str):
        udf_create_statement = cleandoc(f"""
            CREATE OR REPLACE  {language_alias} SET SCRIPT {udf_name.fully_qualified}(ignored INTEGER)
            EMITS (ip_address VARCHAR(10), network_prefix INTEGER) AS
            from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf import RetrieveExasolNodeIPAddressUDF
    
            def run(ctx):
                RetrieveExasolNodeIPAddressUDF().run(ctx)
            /
        """)
        return udf_create_statement
