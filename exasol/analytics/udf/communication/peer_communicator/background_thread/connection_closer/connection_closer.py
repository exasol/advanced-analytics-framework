import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.messages import Message
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.close_connection_sender import \
    CloseConnectionSender
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_is_closed_sender import \
    ConnectionIsClosedSender
from exasol.analytics.udf.communication.peer_communicator.sender import Sender

LOGGER: FilteringBoundLogger = structlog.get_logger()


class ConnectionCloser:
    def __init__(self,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 sender: Sender,
                 abort_timeout_sender: AbortTimeoutSender,
                 connection_is_closed_sender: ConnectionIsClosedSender,
                 close_connection_sender: CloseConnectionSender):
        self._close_connection_sender = close_connection_sender
        self._connection_is_closed_sender = connection_is_closed_sender
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )

    def received_close_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._sender.send(Message(
            __root__=messages.AcknowledgeCloseConnection(
                source=self._my_connection_info,
                destination=self._peer)))
        self._connection_is_closed_sender.received_close_connection()

    def received_acknowledge_close_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._connection_is_closed_sender.received_acknowledge_close_connection()
        self._close_connection_sender.stop()

    def try_send(self):
        self._close_connection_sender.try_send()
        self._connection_is_closed_sender.try_send()
