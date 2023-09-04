import dataclasses


@dataclasses.dataclass
class RegisterPeerForwarderBehaviorConfig:
    needs_to_send_register_peer: bool = False
    needs_to_send_acknowledge_register_peer: bool = False
