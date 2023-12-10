from typing import Tuple

import dill
import pytest

from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.retrieve_exasol_node_ip_address_udf_deployer import \
    RetrieveExasolNodeIPAddressUDFDeployer
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import UDFCommunicatorConfig


@pytest.fixture
def deploy_retrieve_exasol_node_ip_address_udf(setup_database,
                                               pyexasol_connection,
                                               upload_language_container) -> Tuple[str, str]:
    bucketfs_connection_name, schema_name = setup_database
    udf_name = f"RETRIEVE_EXASOL_NODE_IP_ADDRESS"
    udf_create_statement = \
        RetrieveExasolNodeIPAddressUDFDeployer.render_udf_create_statement(
            schema_name=schema_name, udf_name=udf_name)
    pyexasol_connection.execute(udf_create_statement)
    return schema_name, udf_name


@pytest.fixture
def exasol_node_ip_address(pyexasol_connection, deploy_retrieve_exasol_node_ip_address_udf) -> IPAddress:
    schema_name, udf_name = deploy_retrieve_exasol_node_ip_address_udf
    rs = pyexasol_connection.execute(
        f"""
        SELECT {schema_name}.{udf_name}(0) 
        """
    )
    row = rs.fetchone()
    ip_address = IPAddress(ip_address=row[0],
                           network_prefix=row[1])
    return ip_address


@pytest.fixture()
def udf_communicator_config(exasol_node_ip_address) -> UDFCommunicatorConfig:
    config = UDFCommunicatorConfig(
        listen_port=Port(port=16789),
        multi_node_discovery_ip=exasol_node_ip_address,
        group_identifier_suffix="test",
        number_of_instances_per_node=2
    )
    return config


@pytest.fixture()
def udf_communicator_config_connecion(pyexasol_connection, udf_communicator_config) -> str:
    connection_name = f"udf_communicator_config_connecion"
    pyexasol_connection.execute(f"""
    CREATE OR REPLACE CONNECTION {connection_name} TO '{udf_communicator_config.json()}';
    """)
    return connection_name


def udf_function(ctx, exa, port_base, connection_name):
    from exasol_advanced_analytics_framework.udf_communication.ip_address import Port
    from exasol_advanced_analytics_framework.udf_communication.exchange_cluster_information import \
        exchange_cluster_information, WorkerAddress
    from exasol_advanced_analytics_framework.udf_communication.udf_communicator import udf_communicator
    with udf_communicator(exa, connection_name) as communicator:
        ip = communicator.listen_ip
        instance_id = ctx.instance_id
        port = Port(port=port_base + instance_id)
        worker_address = WorkerAddress(ip_address=ip, port=port)
        cluster_information = exchange_cluster_information(communicator, worker_address)
    for worker in cluster_information.workers:
        ctx.emit(instance_id, worker.ip_address.ip_address, worker.port.port)


@pytest.fixture()
def test_udf(
        setup_database,
        pyexasol_connection,
        udf_communicator_config_connecion,
        upload_language_container,
) -> Tuple[str, str, int]:
    bucketfs_connection_name, schema_name = setup_database
    port_base = 20000
    udf_function_source_code = dill.source.getsource(udf_function)
    udf_name = "test_udf"
    language_alias = upload_language_container
    udf = f"""
--/
CREATE OR REPLACE  {language_alias} SET SCRIPT {schema_name}.{udf_name}(instance_id INTEGER)
EMITS (instance_id INTEGER, ip VARCHAR(10), port INTEGER) AS

{udf_function_source_code}

def run(ctx):
    udf_function(ctx, exa, {port_base}, "{udf_communicator_config_connecion}")
/
"""
    pyexasol_connection.execute(udf)
    return schema_name, udf_name, port_base


def test(
        pyexasol_connection,
        udf_communicator_config,
        test_udf
):
    schema_name, udf_name, port_base = test_udf
    number_of_instances_per_node = udf_communicator_config.number_of_instances_per_node
    rs = pyexasol_connection.execute(
        f"""
        SELECT {schema_name}.{udf_name}(t.instance_id) 
        FROM VALUES BETWEEN 1 AND {number_of_instances_per_node} as t(instance_id)
        GROUP BY t.instance_id
        """
    )
    rows = rs.fetchall()
    ip_address = udf_communicator_config.multi_node_discovery_ip.ip_address
    assert set(rows) == {
        (2, ip_address, 20001),
        (2, ip_address, 20002),
        (1, ip_address, 20001),
        (1, ip_address, 20002)
    }
