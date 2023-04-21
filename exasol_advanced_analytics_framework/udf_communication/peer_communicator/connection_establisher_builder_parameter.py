import dataclasses

from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_behavior_config import \
    ConnectionEstablisherBehaviorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config import \
    ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection


@dataclasses.dataclass(frozen=True)
class ConnectionEstablisherBuilderParameter:
    register_peer_connection: RegisterPeerConnection
    behavior_config: ConnectionEstablisherBehaviorConfig
    timeout_config: ConnectionEstablisherTimeoutConfig
