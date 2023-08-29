from typing import List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract \
    import SocketFactory, Frame, SocketType

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 sender: Sender,
                 connection_establisher: ConnectionEstablisher):
        self._connection_establisher = connection_establisher
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._socket_factory = socket_factory
        self._create_receive_socket()
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._logger.debug("__init__")

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    def try_send(self):
        self._logger.debug("try_send")
        self._connection_establisher.try_send()

    def received_synchronize_connection(self):
        self._connection_establisher.received_synchronize_connection()

    def received_acknowledge_connection(self):
        self._connection_establisher.received_acknowledge_connection()

    def received_acknowledge_register_peer(self):
        self._connection_establisher.received_acknowledge_register_peer()

    def received_register_peer_complete(self):
        self._connection_establisher.received_register_peer_complete()

    def forward_payload(self, frames: List[Frame]):
        self._receive_socket.send_multipart(frames)

    def stop(self):
        self._receive_socket.close(linger=0)

    def is_ready_to_stop(self):
        return self._connection_establisher.is_ready_to_stop()
