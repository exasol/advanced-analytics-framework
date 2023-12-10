import contextlib

import structlog
from pydantic import BaseModel
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator, CommunicatorFactory
from exasol_advanced_analytics_framework.udf_communication.host_ip_addresses import HostIPAddresses
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port, are_ips_in_same_network
from exasol_advanced_analytics_framework.udf_communication.socket_factory_context_manager_factory import \
    SocketFactoryContextManagerFactory
from exasol_advanced_analytics_framework.udf_communication.zmq_socket_factory_context_manager_factory import \
    ZMQSocketFactoryContextManagerFactory

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class UDFCommunicatorConfig(BaseModel):
    multi_node_discovery_ip: IPAddress
    listen_port: Port
    number_of_instances_per_node: int
    group_identifier_suffix: str


@contextlib.contextmanager
def udf_communicator(exa, connection_name: str,
                     host_ip_addresses: HostIPAddresses = HostIPAddresses(),
                     communicator_factory: CommunicatorFactory = CommunicatorFactory(),
                     socket_factory_context_manager_factory: SocketFactoryContextManagerFactory =
                     ZMQSocketFactoryContextManagerFactory()
                     ) -> Communicator:
    connection = exa.get_connection(connection_name)
    config = UDFCommunicatorConfig.parse_raw(connection.address)
    with socket_factory_context_manager_factory.create() as socket_factory:
        node_name = exa.meta.node_id
        instance_name = exa.meta.vm_id
        group_identifier = f"{exa.meta.session_id}_{exa.meta.statement_id}_{config.group_identifier_suffix}"
        number_of_nodes = exa.meta.node_count
        host_ip_address = find_host_ip_address_in_multi_node_discovery_subnet(config, host_ip_addresses)
        is_discovery_leader_node = host_ip_address == config.multi_node_discovery_ip
        communicator = communicator_factory.create(
            multi_node_discovery_ip=config.multi_node_discovery_ip,
            socket_factory=socket_factory,
            node_name=node_name,
            instance_name=instance_name,
            listen_ip=host_ip_address,
            group_identifier=group_identifier,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=config.number_of_instances_per_node,
            is_discovery_leader_node=is_discovery_leader_node,
            multi_node_discovery_port=config.listen_port,
            local_discovery_port=config.listen_port
        )
        try:
            yield communicator
        finally:
            communicator.stop()


def find_host_ip_address_in_multi_node_discovery_subnet(
        config: UDFCommunicatorConfig, host_ip_addresses: HostIPAddresses):
    ip_addresses = host_ip_addresses.get_all_ip_addresses()
    LOGGER.info("find_host_ip_address_in_multi_node_discovery_subnet",
                ip_addresses=ip_addresses, config=config.dict())
    for host_ip_address in ip_addresses:
        if are_ips_in_same_network(config.multi_node_discovery_ip, host_ip_address):
            return host_ip_address
    raise RuntimeError("No compatible IP address found")
