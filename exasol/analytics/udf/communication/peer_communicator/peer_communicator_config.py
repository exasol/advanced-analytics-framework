import dataclasses

from exasol.analytics.udf.communication.peer_communicator. \
    background_thread.connection_closer.connection_closer_timeout_config import ConnectionCloserTimeoutConfig
from exasol.analytics.udf.communication.peer_communicator. \
    connection_establisher_timeout_config import ConnectionEstablisherTimeoutConfig
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import \
    ForwardRegisterPeerConfig
from exasol.analytics.udf.communication.peer_communicator. \
    payload_message_sender_timeout_config import PayloadMessageSenderTimeoutConfig
from exasol.analytics.udf.communication.peer_communicator. \
    register_peer_forwarder_timeout_config import RegisterPeerForwarderTimeoutConfig


@dataclasses.dataclass
class PeerCommunicatorConfig:
    connection_establisher_timeout_config: ConnectionEstablisherTimeoutConfig = ConnectionEstablisherTimeoutConfig()
    connection_closer_timeout_config: ConnectionCloserTimeoutConfig = ConnectionCloserTimeoutConfig()
    register_peer_forwarder_timeout_config: RegisterPeerForwarderTimeoutConfig = RegisterPeerForwarderTimeoutConfig()
    forward_register_peer_config: ForwardRegisterPeerConfig = ForwardRegisterPeerConfig()
    payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig = PayloadMessageSenderTimeoutConfig()
    poll_timeout_in_ms: int = 200
    send_socket_linger_time_in_ms: int = 100
    close_timeout_in_ms: int = 100000
