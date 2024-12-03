from exasol.analytics.udf.communication.discovery.localhost.discovery_socket import (
    DiscoverySocketFactory,
)
from exasol.analytics.udf.communication.discovery.localhost.discovery_strategy import (
    DiscoveryStrategy,
)
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import (
    ForwardRegisterPeerConfig,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.socket_factory.abstract import SocketFactory


class CommunicatorFactory:

    def create(
        self,
        name: str,
        group_identifier: str,
        number_of_instances: int,
        listen_ip: IPAddress,
        discovery_port: Port,
        socket_factory: SocketFactory,
        discovery_socket_factory: DiscoverySocketFactory,
    ) -> PeerCommunicator:
        peer_communicator = PeerCommunicator(
            name=name,
            number_of_peers=number_of_instances,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=False,
                    is_enabled=False,
                )
            ),
            socket_factory=socket_factory,
        )
        discovery = DiscoveryStrategy(
            port=discovery_port,
            timeout_in_seconds=120,
            time_between_ping_messages_in_seconds=1,
            peer_communicator=peer_communicator,
            local_discovery_socket_factory=discovery_socket_factory,
        )
        discovery.discover_peers()
        return peer_communicator
