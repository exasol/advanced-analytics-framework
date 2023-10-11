import contextlib
from ipaddress import ip_network
from typing import List

import structlog
import zmq
from pydantic import BaseModel
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_wrapper import ZMQSocketFactory

import ifaddr

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class UDFCommunicatorConfig(BaseModel):
    multi_node_discovery_ip: IPAddress
    listen_port: Port
    number_of_instances_per_node: int
    group_identifier_suffix: str


class HostIPAddresses:
    def get_all_ip_addresses(self) -> List[IPAddress]:
        ip_adresses = [IPAddress(ip_address=ip.ip, network_prefix=ip.network_prefix)
                       for adapter in ifaddr.get_adapters()
                       for ip in adapter.ips
                       if ip.is_IPv4 and ip.ip]
        return ip_adresses


@contextlib.contextmanager
def udf_communicator(exa, connection_name: str, host_ip_addresses: HostIPAddresses = HostIPAddresses()) -> Communicator:
    connection = exa.get_connection(connection_name)
    config = UDFCommunicatorConfig.parse_raw(connection.address)
    context = zmq.Context()
    socket_factory = ZMQSocketFactory(context)
    listen_ip = IPAddress(ip_address="0.0.0.0")
    node_name = exa.meta.node_id
    instance_name = exa.meta.vm_id
    group_identifier = f"{exa.meta.session_id}_{exa.meta.statement_id}_{config.group_identifier_suffix}"
    number_of_nodes = exa.meta.node_count
    host_ip_address = find_host_ip_address_in_multi_node_discovery_subnet(config, host_ip_addresses)
    is_discovery_leader_node = host_ip_address == config.multi_node_discovery_ip
    communicator = Communicator(
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
    yield communicator
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


def are_ips_in_same_network(ip_address1: IPAddress, ip_address2: IPAddress):
    ip_network1 = ip_network(f"{ip_address1.ip_address}/{ip_address1.network_prefix}", strict=False)
    ip_network2 = ip_network(f"{ip_address2.ip_address}/{ip_address2.network_prefix}", strict=False)
    return ip_network1.network_address == ip_network2.network_address
