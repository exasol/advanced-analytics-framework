import dataclasses

from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config import \
    ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.forward_register_peer_config import \
    ForwardRegisterPeerConfig


@dataclasses.dataclass
class PeerCommunicatorConfig:
    connection_establisher_timeout_config: ConnectionEstablisherTimeoutConfig = ConnectionEstablisherTimeoutConfig()
    forward_register_peer_config: ForwardRegisterPeerConfig = ForwardRegisterPeerConfig()
    poll_timeout_in_ms: int = 200
    send_socket_linger_time_in_ms: int = 100
    close_timeout_in_ms: int = 100000
