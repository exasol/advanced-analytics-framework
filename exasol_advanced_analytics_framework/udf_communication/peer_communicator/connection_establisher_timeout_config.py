import dataclasses


@dataclasses.dataclass
class ConnectionEstablisherTimeoutConfig:
    synchronize_retry_timeout_in_ms: int = 1000
    abort_timeout_in_ms: int = 100000
    peer_is_ready_wait_time_in_ms: int = 10000
    register_peer_retry_timeout_in_ms: int = 1000
    acknowledge_register_peer_retry_timeout_in_ms: int = 1000
