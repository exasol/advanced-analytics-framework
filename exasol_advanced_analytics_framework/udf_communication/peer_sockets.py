import zmq

from exasol_advanced_analytics_framework.udf_communication.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class PeerSockets:

    def __init__(self, context: zmq.Context, peer: Peer):
        self.peer = peer
        self._create_send_socket(context, peer)
        self._create_receive_socket(context, peer)

    def _create_receive_socket(self, context, peer):
        self.receive_socket = context.socket(zmq.PAIR)
        receive_socket_address = get_peer_receive_socket_name(peer)
        self.receive_socket.bind(receive_socket_address)

    def _create_send_socket(self, context, peer):
        self.send_socket = context.socket(zmq.DEALER)
        self.send_socket.connect(f"tcp://{peer.connection_info.ipaddress.ip_address}:{peer.connection_info.port.port}")
