import dataclasses

from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_behavior_config \
    import RegisterPeerForwarderBehaviorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_timeout_config \
    import RegisterPeerForwarderTimeoutConfig


@dataclasses.dataclass(frozen=True)
class RegisterPeerForwarderBuilderParameter:
    register_peer_connection: RegisterPeerConnection
    behavior_config: RegisterPeerForwarderBehaviorConfig
    timeout_config: RegisterPeerForwarderTimeoutConfig
