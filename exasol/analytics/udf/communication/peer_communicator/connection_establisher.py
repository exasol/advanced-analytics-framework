import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.messages import Message
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import (
    AbortTimeoutSender,
)
from exasol.analytics.udf.communication.peer_communicator.connection_is_ready_sender import (
    ConnectionIsReadySender,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.synchronize_connection_sender import (
    SynchronizeConnectionSender,
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


class ConnectionEstablisher:
    def __init__(
        self,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        sender: Sender,
        abort_timeout_sender: AbortTimeoutSender,
        connection_is_ready_sender: ConnectionIsReadySender,
        synchronize_connection_sender: SynchronizeConnectionSender,
    ):
        self._synchronize_connection_sender = synchronize_connection_sender
        self._connection_is_ready_sender = connection_is_ready_sender
        self._abort_timeout_sender = abort_timeout_sender
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.model_dump(),
            my_connection_info=self._my_connection_info.model_dump(),
        )
        self._send_initial_messages()

    def _send_initial_messages(self):
        self._synchronize_connection_sender.try_send(force=True)

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._sender.send(
            Message(
                root=messages.AcknowledgeConnection(
                    source=self._my_connection_info, destination=self._peer
                )
            )
        )
        self._connection_is_ready_sender.received_synchronize_connection()
        self._abort_timeout_sender.stop()

    def received_acknowledge_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._connection_is_ready_sender.received_acknowledge_connection()
        self._synchronize_connection_sender.stop()
        self._abort_timeout_sender.stop()

    def try_send(self):
        self._synchronize_connection_sender.try_send()
        self._abort_timeout_sender.try_send()
        self._connection_is_ready_sender.try_send()

    def is_ready_to_stop(self):
        return self._connection_is_ready_sender.is_ready_to_stop()
