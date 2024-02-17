import textwrap

from exasol_data_science_utils_python.schema.udf_name import UDFName


class RetrieveExasolNodeIPAddressUDFDeployer:

    def render_udf_create_statement(self, udf_name: UDFName):
        udf_create_statement = textwrap.dedent(f"""
            CREATE OR REPLACE  PYTHON3_AAF SET SCRIPT {udf_name.fully_qualified}(ignored INTEGER)
            EMITS (ip_address VARCHAR(10), network_prefix INTEGER) AS
            from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf import \
                RetrieveExasolNodeIPAddressUDF
    
            def run(ctx):
                RetrieveExasolNodeIPAddressUDF().run(ctx)
            /
        """)
        return udf_create_statement
