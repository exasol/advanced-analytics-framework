import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import (
    AbortTimeoutSender,
)
from exasol.analytics.udf.communication.peer_communicator.acknowledge_register_peer_sender import (
    AcknowledgeRegisterPeerSender,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_connection import (
    RegisterPeerConnection,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_is_ready_sender import (
    RegisterPeerForwarderIsReadySender,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_sender import (
    RegisterPeerSender,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender

LOGGER: FilteringBoundLogger = structlog.get_logger()


class RegisterPeerForwarder:

    def __init__(
        self,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        sender: Sender,
        register_peer_connection: RegisterPeerConnection,
        abort_timeout_sender: AbortTimeoutSender,
        acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender,
        register_peer_sender: RegisterPeerSender,
        register_peer_forwarder_is_ready_sender: RegisterPeerForwarderIsReadySender,
    ):
        self._register_peer_forwarder_is_ready_sender = (
            register_peer_forwarder_is_ready_sender
        )
        self._register_peer_sender = register_peer_sender
        self._acknowledge_register_peer_sender = acknowledge_register_peer_sender
        self._abort_timeout_sender = abort_timeout_sender
        self._register_peer_connection = register_peer_connection
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._send_initial_messages()

    def _send_initial_messages(self):
        self._register_peer_sender.try_send(force=True)
        self._acknowledge_register_peer_sender.try_send(force=True)
        self._register_peer_forwarder_is_ready_sender.try_send()

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._register_peer_connection.complete(self._peer)
        self._register_peer_sender.stop()
        self._abort_timeout_sender.stop()
        self._register_peer_forwarder_is_ready_sender.received_acknowledge_register_peer()

    def received_register_peer_complete(self):
        self._logger.debug("received_register_peer_complete")
        self._acknowledge_register_peer_sender.stop()
        self._register_peer_forwarder_is_ready_sender.received_register_peer_complete()

    def try_send(self):
        self._register_peer_sender.try_send()
        self._acknowledge_register_peer_sender.try_send()
        self._abort_timeout_sender.try_send()
        self._register_peer_forwarder_is_ready_sender.try_send()

    def is_ready_to_stop(self):
        return self._register_peer_forwarder_is_ready_sender.is_ready_to_stop()
