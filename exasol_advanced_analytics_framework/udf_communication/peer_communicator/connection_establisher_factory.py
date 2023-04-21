from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender


class ConnectionEstablisherFactory:

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               sender: Sender,
               register_peer_connection: RegisterPeerConnection,
               abort_timeout_sender: AbortTimeoutSender,
               acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender,
               peer_is_ready_sender: PeerIsReadySender,
               register_peer_sender: RegisterPeerSender,
               synchronize_connection_sender: SynchronizeConnectionSender
               ) -> ConnectionEstablisher:
        return ConnectionEstablisher(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            register_peer_connection=register_peer_connection,
            abort_timeout_sender=abort_timeout_sender,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender,
            peer_is_ready_sender=peer_is_ready_sender,
            register_peer_sender=register_peer_sender,
            synchronize_connection_sender=synchronize_connection_sender
        )
