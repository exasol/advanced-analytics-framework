from typing import Optional, Dict, List, Set, cast

import zmq
from sortedcontainers import SortedSet

from exasol_advanced_analytics_framework.udf_communication.background_listener import BackgroundListener
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import PongMessage, PayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_sockets import PeerSockets
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message


def key_for_peer(peer: Peer):
    return peer.connection_info.ipaddress.ip_address + "_" + str(peer.connection_info.port.port)


class PeerCommunicator:

    def __init__(self, number_of_peers: int, listen_ip: IPAddress, group_identifier: str):
        self._number_of_peers = number_of_peers
        self._context = zmq.Context()
        self._background_listener = BackgroundListener(context=self._context,
                                                       listen_ip=listen_ip,
                                                       group_identifier=group_identifier)
        self._my_connection_info = self._background_listener.my_connection_info
        self._peer_sockets: Dict[Peer, PeerSockets] = {}

        self._sorted_peers: Set[Peer] = cast(Set[Peer], SortedSet(key=key_for_peer))

    def _update_peers(self):
        for pong_message in self._background_listener.receive_pong_messages():
            peer = Peer(connection_info=pong_message.connection_info)
            self._register_peer(peer)
            self._add_peer(peer)

    def wait_for_peers(self):
        while not self.are_all_peers_connected():
            pass

    def peers(self) -> List[Peer]:
        self.wait_for_peers()
        return list(self._sorted_peers)

    def register_peer(self, peer_connection_info: ConnectionInfo):
        if (peer_connection_info.group_identifier == self._my_connection_info.group_identifier
                and peer_connection_info != self._my_connection_info):
            peer = Peer(connection_info=peer_connection_info)
            self._register_peer(peer)

    def _register_peer(self, peer):
        if peer not in self._peer_sockets:
            peer_sockets = PeerSockets(self._context, peer)
            self._peer_sockets[peer] = peer_sockets
            self._background_listener.register_peer(peer)
            # TODO: Here we can get a race condition, when the PongMessage arrives before the register_peer is completed
            # We would need a way to wait for register_peer via an ack.
            # To be able to ack control messages, we need two control sockets.
            # With that we need a event loop also in PeerCommuincation which runs when ever it is used.
            # It needs to handle more then only the PongMessages.
            pong_message = PongMessage(connection_info=self.my_connection_info)
            peer_sockets.send_socket.send(serialize_message(pong_message))

    def _add_peer(self, peer: Peer):
        self._sorted_peers.add(peer)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def are_all_peers_connected(self) -> bool:
        self._update_peers()
        return len(self._sorted_peers) == self._number_of_peers - 1

    def send(self, peer: Peer, message: bytes):
        assert self.are_all_peers_connected()
        payload_message = PayloadMessage(
            connection_info=self.my_connection_info
        )
        self._peer_sockets[peer].send_socket.send_multipart([serialize_message(payload_message), message])

    def recv(self, peer: Peer, timeout: Optional[int] = None) -> bytes:
        assert self.are_all_peers_connected()
        receive_socket = self._peer_sockets[peer].receive_socket
        if receive_socket.poll(flags=zmq.POLLIN, timeout=timeout) != 0:
            return receive_socket.recv()

    def __del__(self):
        self._background_listener.stop()
