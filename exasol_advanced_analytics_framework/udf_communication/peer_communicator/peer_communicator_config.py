import dataclasses

from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config \
    import ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.forward_register_peer_config \
    import ForwardRegisterPeerConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_timeout_config \
    import PayloadMessageSenderTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_timeout_config \
    import RegisterPeerForwarderTimeoutConfig


@dataclasses.dataclass
class PeerCommunicatorConfig:
    connection_establisher_timeout_config: ConnectionEstablisherTimeoutConfig = ConnectionEstablisherTimeoutConfig()
    register_peer_forwarder_timeout_config: RegisterPeerForwarderTimeoutConfig = RegisterPeerForwarderTimeoutConfig()
    payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig = PayloadMessageSenderTimeoutConfig()
    forward_register_peer_config: ForwardRegisterPeerConfig = ForwardRegisterPeerConfig()
    poll_timeout_in_ms: int = 200
    send_socket_linger_time_in_ms: int = 100
    close_timeout_in_ms: int = 100000
