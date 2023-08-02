from exasol_advanced_analytics_framework.udf_communication.discovery import multi_node
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory


class CommunicatorFactory:

    def create(
            self,
            name: str,
            group_identifier: str,
            is_discovery_leader: bool,
            number_of_instances: int,
            listen_ip: IPAddress,
            discovery_ip: IPAddress,
            discovery_port: Port,
            socket_factory: SocketFactory,
            discovery_socket_factory: multi_node.DiscoverySocketFactory) -> PeerCommunicator:
        peer_communicator = PeerCommunicator(
            name=name,
            number_of_peers=number_of_instances,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            is_forward_register_peer_leader=is_discovery_leader,
            is_forward_register_peer_enabled=True,
            socket_factory=socket_factory
        )
        discovery = multi_node.DiscoveryStrategy(
            ip_address=discovery_ip,
            port=discovery_port,
            timeout_in_seconds=120,
            time_between_ping_messages_in_seconds=1,
            peer_communicator=peer_communicator,
            global_discovery_socket_factory=discovery_socket_factory,
        )
        discovery.discover_peers()
        return peer_communicator
