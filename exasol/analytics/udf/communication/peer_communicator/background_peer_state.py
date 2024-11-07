from typing import List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator. \
    background_thread.connection_closer.connection_closer import ConnectionCloser
from exasol.analytics.udf.communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol.analytics.udf.communication.peer_communicator.payload_handler import PayloadHandler
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder import \
    RegisterPeerForwarder
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.socket_factory.abstract \
    import Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundPeerState:

    def __init__(
            self,
            my_connection_info: ConnectionInfo,
            peer: Peer,
            sender: Sender,
            connection_establisher: ConnectionEstablisher,
            connection_closer: ConnectionCloser,
            register_peer_forwarder: RegisterPeerForwarder,
            payload_handler: PayloadHandler
    ):
        self._connection_closer = connection_closer
        self._payload_handler = payload_handler
        self._register_peer_forwarder = register_peer_forwarder
        self._connection_establisher = connection_establisher
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._prepare_to_stop = False
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._logger.debug("__init__")

    def try_send(self):
        self._logger.debug("try_send")
        self._connection_establisher.try_send()
        self._register_peer_forwarder.try_send()
        self._payload_handler.try_send()
        if self._should_we_close_connection():
            self._connection_closer.try_send()

    def _should_we_close_connection(self):
        is_ready_to_stop = self._is_ready_to_stop()
        self._logger.debug("_should_we_send_close_connection",
                           is_ready_to_stop=is_ready_to_stop,
                           prepare_to_stop=self._prepare_to_stop)
        return self._prepare_to_stop and is_ready_to_stop

    def received_synchronize_connection(self):
        self._connection_establisher.received_synchronize_connection()

    def received_acknowledge_connection(self):
        self._connection_establisher.received_acknowledge_connection()

    def received_acknowledge_register_peer(self):
        self._register_peer_forwarder.received_acknowledge_register_peer()

    def received_register_peer_complete(self):
        self._register_peer_forwarder.received_register_peer_complete()

    def send_payload(self, message: messages.Payload, frames: List[Frame]):
        self._payload_handler.send_payload(message, frames)

    def received_payload(self, message: messages.Payload, frames: List[Frame]):
        self._payload_handler.received_payload(message, frames)

    def received_acknowledge_payload(self, message: messages.AcknowledgePayload):
        self._payload_handler.received_acknowledge_payload(message=message)

    def prepare_to_stop(self):
        self._logger.info("prepare_to_stop")
        self._prepare_to_stop = True

    def _is_ready_to_stop(self):
        connection_establisher_is_ready = self._connection_establisher.is_ready_to_stop()
        register_peer_forwarder_is_ready = self._register_peer_forwarder.is_ready_to_stop()
        payload_handler_is_ready = self._payload_handler.is_ready_to_stop()
        is_ready_to_stop = (
                connection_establisher_is_ready
                and register_peer_forwarder_is_ready
                and payload_handler_is_ready
        )
        self._logger.debug("background_peer_state_is_ready_to_stop",
                           connection_establisher_is_ready=connection_establisher_is_ready,
                           register_peer_forwarder_is_ready=register_peer_forwarder_is_ready,
                           payload_handler_is_ready=payload_handler_is_ready)
        return is_ready_to_stop

    def received_close_connection(self):
        self._connection_closer.received_close_connection()

    def received_acknowledge_close_connection(self):
        self._connection_closer.received_acknowledge_close_connection()
