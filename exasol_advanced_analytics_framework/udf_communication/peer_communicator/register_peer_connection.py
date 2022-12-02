from typing import Optional

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import RegisterPeerMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.send_socket_factory import \
    SendSocketFactory
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket


class RegisterPeerConnection:

    def __init__(self,
                 predecessor_send_socket_factory: Optional[SendSocketFactory],
                 successor_send_socket_factory: SendSocketFactory,
                 my_connection_info: ConnectionInfo, ):
        self._my_connection_info = my_connection_info
        self._successor_socket = successor_send_socket_factory.create_send_socket()
        self._predecessor: Optional[Socket] = None
        if predecessor_send_socket_factory is not None:
            self._predecessor_socket = predecessor_send_socket_factory.create_send_socket()

    def forward(self, peer: Peer):
        message = RegisterPeerMessage(
            peer=peer,
            source=Peer(connection_info=self._my_connection_info)
        )
        serialized_message = serialize_message(message)
        self._successor_socket.send(serialized_message)

    def ack(self, peer: Peer):
        raise NotImplementedError()

    def close(self):
        self._successor_socket.close()
        if self._predecessor is not None:
            self._predecessor.close()

    def __del__(self):
        self.close()
