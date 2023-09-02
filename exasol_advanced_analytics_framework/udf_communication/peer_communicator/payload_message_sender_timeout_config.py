import dataclasses


@dataclasses.dataclass
class PayloadMessageSenderTimeoutConfig:
    abort_timeout_in_ms: int = 10000
    retry_timeout_in_ms: int = 1000
