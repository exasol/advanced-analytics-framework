import zmq

from exasol_advanced_analytics_framework.udf_communication.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class PeerSockets:

    def __init__(self, context: zmq.Context, peer: Peer):
        self._peer = peer
        self._context = context
        self._create_receive_socket()

    def _create_receive_socket(self):
        self._receive_socket = self._context.socket(zmq.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    def create_send_socket(self) -> zmq.Socket:
        send_socket = self._context.socket(zmq.DEALER)
        send_socket.connect(
            f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
        return send_socket

    @property
    def receive_socket(self) -> zmq.Socket:
        return self._receive_socket

