from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator. \
    background_thread.connection_closer.close_connection_sender import CloseConnectionSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator. \
    background_thread.connection_closer.connection_closer import ConnectionCloser
from exasol_advanced_analytics_framework.udf_communication.peer_communicator. \
    background_thread.connection_closer.connection_is_closed_sender import ConnectionIsClosedSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender


class ConnectionCloserFactory:

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               sender: Sender,
               abort_timeout_sender: AbortTimeoutSender,
               connection_is_closed_sender: ConnectionIsClosedSender,
               close_connection_sender: CloseConnectionSender
               ) -> ConnectionCloser:
        return ConnectionCloser(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            abort_timeout_sender=abort_timeout_sender,
            connection_is_closed_sender=connection_is_closed_sender,
            close_connection_sender=close_connection_sender
        )
