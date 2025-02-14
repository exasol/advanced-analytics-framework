import dataclasses


@dataclasses.dataclass(frozen=True)
class ConnectionCloserTimeoutConfig:
    close_retry_timeout_in_ms: int = 1000
    abort_timeout_in_ms: int = 100000
    connection_is_closed_wait_time_in_ms: int = 10000
