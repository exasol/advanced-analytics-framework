from exasol_advanced_analytics_framework.udf_communication.discovery import localhost
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory


def create_local_peer_communicator(
        name: str,
        group_identifier: str,
        number_of_instances: int,
        listen_ip: IPAddress,
        discovery_port: Port,
        socket_factory: SocketFactory,
        discovery_socket_factory: localhost.DiscoverySocketFactory):
    peer_communicator = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=listen_ip,
        group_identifier=group_identifier,
        is_forward_register_peer_leader=False,
        is_forward_register_peer_enabled=False,
        socket_factory=socket_factory
    )
    discovery = localhost.DiscoveryStrategy(
        port=discovery_port,
        timeout_in_seconds=120,
        time_between_ping_messages_in_seconds=1,
        peer_communicator=peer_communicator,
        local_discovery_socket_factory=discovery_socket_factory,
    )
    discovery.discover_peers()
    return peer_communicator
