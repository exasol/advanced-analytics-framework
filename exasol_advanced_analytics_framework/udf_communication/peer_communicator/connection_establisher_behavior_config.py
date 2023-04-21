import dataclasses


@dataclasses.dataclass
class ConnectionEstablisherBehaviorConfig:
    forward_register_peer: bool = False
    acknowledge_register_peer: bool = False
    needs_register_peer_complete: bool = False
