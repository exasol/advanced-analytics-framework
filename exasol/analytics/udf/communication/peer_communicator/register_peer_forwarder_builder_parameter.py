import dataclasses
from typing import Optional

from exasol.analytics.udf.communication.peer_communicator.register_peer_connection import (
    RegisterPeerConnection,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_behavior_config import (
    RegisterPeerForwarderBehaviorConfig,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_timeout_config import (
    RegisterPeerForwarderTimeoutConfig,
)


@dataclasses.dataclass(frozen=True)
class RegisterPeerForwarderBuilderParameter:
    register_peer_connection: Optional[RegisterPeerConnection]
    behavior_config: RegisterPeerForwarderBehaviorConfig
    timeout_config: RegisterPeerForwarderTimeoutConfig
