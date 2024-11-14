import dataclasses


@dataclasses.dataclass(frozen=True)
class ForwardRegisterPeerConfig:
    is_leader: bool = False
    is_enabled: bool = False
