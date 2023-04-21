from exasol_advanced_analytics_framework.udf_communication.global_discovery_socket import GlobalDiscoverySocketFactory
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocketFactory
from exasol_advanced_analytics_framework.udf_communication.local_discovery_strategy import LocalDiscoveryStrategy
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.forward_register_peer_config import \
    ForwardRegisterPeerConfig
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketFactory


def create_local_peer_communicator(
        name: str,
        group_identifier: str,
        number_of_instances: int,
        listen_ip: IPAddress,
        discovery_port: Port,
        socket_factory: SocketFactory,
        local_discovery_socket_factory: LocalDiscoverySocketFactory):
    peer_communicator = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=listen_ip,
        group_identifier=group_identifier,
        forward_register_peer_config=ForwardRegisterPeerConfig(
            is_leader=False,
            is_enabled=False
        ),
        socket_factory=socket_factory
    )
    discovery = LocalDiscoveryStrategy(
        port=discovery_port,
        timeout_in_seconds=120,
        time_between_ping_messages_in_seconds=1,
        peer_communicator=peer_communicator,
        local_discovery_socket_factory=local_discovery_socket_factory,
    )
    discovery.discover_peers()
    return peer_communicator
