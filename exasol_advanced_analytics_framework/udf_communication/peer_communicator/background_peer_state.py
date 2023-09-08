from typing import List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler import PayloadHandler
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder import \
    RegisterPeerForwarder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract \
    import Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundPeerState:

    def __init__(
            self,
            my_connection_info: ConnectionInfo,
            peer: Peer,
            sender: Sender,
            connection_establisher: ConnectionEstablisher,
            register_peer_forwarder: RegisterPeerForwarder,
            payload_handler: PayloadHandler
    ):
        self._payload_handler = payload_handler
        self._register_peer_forwarder = register_peer_forwarder
        self._connection_establisher = connection_establisher
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._logger.debug("__init__")

    def try_send(self):
        self._logger.debug("try_send")
        self._connection_establisher.try_send()
        self._register_peer_forwarder.try_send()

    def received_synchronize_connection(self):
        self._connection_establisher.received_synchronize_connection()

    def received_acknowledge_connection(self):
        self._connection_establisher.received_acknowledge_connection()

    def received_acknowledge_register_peer(self):
        self._register_peer_forwarder.received_acknowledge_register_peer()

    def received_register_peer_complete(self):
        self._register_peer_forwarder.received_register_peer_complete()

    def is_ready_to_stop(self):
        is_ready_to_stop = (
                self._connection_establisher.is_ready_to_stop()
                and self._register_peer_forwarder.is_ready_to_stop()
        )
        return is_ready_to_stop

    def send_payload(self, frames: List[Frame]):
        self._sender.send_multipart(frames)

    def stop(self):
        pass
