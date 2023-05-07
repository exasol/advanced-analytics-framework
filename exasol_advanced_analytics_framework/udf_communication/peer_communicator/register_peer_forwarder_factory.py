from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder import \
    RegisterPeerForwarder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_is_ready_sender \
    import RegisterPeerForwarderIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender


class RegisterPeerForwarderFactory:

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               sender: Sender,
               register_peer_connection: RegisterPeerConnection,
               abort_timeout_sender: AbortTimeoutSender,
               acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender,
               register_peer_sender: RegisterPeerSender,
               register_peer_forwarder_is_ready_sender: RegisterPeerForwarderIsReadySender) -> RegisterPeerForwarder:
        return RegisterPeerForwarder(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            register_peer_connection=register_peer_connection,
            abort_timeout_sender=abort_timeout_sender,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender,
            register_peer_sender=register_peer_sender,
            register_peer_forwarder_is_ready_sender=register_peer_forwarder_is_ready_sender
        )
