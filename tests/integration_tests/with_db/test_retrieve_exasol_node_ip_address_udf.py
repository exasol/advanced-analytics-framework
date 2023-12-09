from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf_deployer import \
    RetrieveExasolNodeIPAddressUDFDeployer


def test(setup_database, pyexasol_connection, upload_language_container):
    bucketfs_connection_name, schema_name = setup_database
    pyexasol_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    udf_name = "RETRIEVE_EXASOL_NODE_IP_ADDRESS"
    udf_create_statement = \
        RetrieveExasolNodeIPAddressUDFDeployer.render_udf_create_statement(
            schema_name=schema_name, udf_name=udf_name)
    pyexasol_connection.execute(udf_create_statement)
    rs = pyexasol_connection.execute(
        f"""
        SELECT {schema_name}.{udf_name}(0) 
        """
    )
    assert rs.rowcount() == 1
