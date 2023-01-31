from exasol_advanced_analytics_framework.udf_communication.global_discovery_socket import GlobalDiscoverySocketFactory
from exasol_advanced_analytics_framework.udf_communication.global_discovery_strategy import GlobalDiscoveryStrategy
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketFactory


def create_global_peer_communicator(
        name: str,
        group_identifier: str,
        is_leader: bool,
        number_of_instances: int,
        listen_ip: IPAddress,
        discovery_port: Port,
        socket_factory: SocketFactory,
        global_discovery_socket_factory: GlobalDiscoverySocketFactory):
    global_discovery_socket = global_discovery_socket_factory.create(ip_address=listen_ip, port=discovery_port)
    peer_communicator = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=listen_ip,
        group_identifier=group_identifier,
        is_forward_register_peer_leader=is_leader,
        is_forward_register_peer_enabled=True,
        socket_factory=socket_factory
    )
    discovery = GlobalDiscoveryStrategy(
        discovery_timeout_in_seconds=120,
        time_between_ping_messages_in_seconds=1,
        global_discovery_socket=global_discovery_socket,
    )
    discovery.discover_peers(peer_communicator)
    return peer_communicator
