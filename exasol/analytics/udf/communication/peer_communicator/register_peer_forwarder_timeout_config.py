import dataclasses


@dataclasses.dataclass
class RegisterPeerForwarderTimeoutConfig:
    abort_timeout_in_ms: int = 100000
    register_peer_forwarder_is_ready_wait_time_in_ms: int = 10000
    register_peer_retry_timeout_in_ms: int = 1000
    acknowledge_register_peer_retry_timeout_in_ms: int = 1000
