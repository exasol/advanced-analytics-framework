import dataclasses


@dataclasses.dataclass(frozen=True)
class PayloadMessageSenderTimeoutConfig:
    abort_timeout_in_ms: int = 10000
    retry_timeout_in_ms: int = 200
