from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol.analytics.udf.communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol.analytics.udf.communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySender
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender


class ConnectionEstablisherFactory:

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               sender: Sender,
               abort_timeout_sender: AbortTimeoutSender,
               connection_is_ready_sender: ConnectionIsReadySender,
               synchronize_connection_sender: SynchronizeConnectionSender
               ) -> ConnectionEstablisher:
        return ConnectionEstablisher(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            abort_timeout_sender=abort_timeout_sender,
            connection_is_ready_sender=connection_is_ready_sender,
            synchronize_connection_sender=synchronize_connection_sender
        )
