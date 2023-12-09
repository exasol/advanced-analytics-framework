import textwrap


class RetrieveExasolNodeIPAddressUDFDeployer:

    def render_udf_create_statement(schema_name: str, udf_name: str):
        udf_create_statement = textwrap.dedent(f"""
            CREATE OR REPLACE  PYTHON3_AAF SET SCRIPT {schema_name}.{udf_name}(ignored INTEGER)
            EMITS (o VARCHAR(10)) AS
            from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf import \
                RetrieveExasolNodeIPAddressUDF
    
            def run(ctx):
                RetrieveExasolNodeIPAddressUDF().run(ctx)
            /
        """)
        return udf_create_statement
