from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.udf_name import UDFName
from exasol_data_science_utils_python.schema.udf_name_builder import UDFNameBuilder

from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf_deployer import \
    RetrieveExasolNodeIPAddressUDFDeployer


def test():
    udf_name = UDFNameBuilder().create(name="RETRIEVE_EXASOL_NODE_IP_ADDRESS",
                                       schema=SchemaName("MY_SCHEMA"))
    udf_create_statement = \
        RetrieveExasolNodeIPAddressUDFDeployer().render_udf_create_statement(udf_name=udf_name,
                                                                             language_alias="PYTHON3_AAF")
    assert udf_create_statement == """
CREATE OR REPLACE  PYTHON3_AAF SET SCRIPT "MY_SCHEMA"."RETRIEVE_EXASOL_NODE_IP_ADDRESS"(ignored INTEGER)
EMITS (ip_address VARCHAR(10), network_prefix INTEGER) AS
from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf import RetrieveExasolNodeIPAddressUDF

def run(ctx):
    RetrieveExasolNodeIPAddressUDF().run(ctx)
/
"""