import time
from typing import Optional, Dict, List, Set, cast

import zmq
from sortedcontainers import SortedSet

from exasol_advanced_analytics_framework.udf_communication.background_listener import BackgroundListener
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import PongMessage, PayloadMessage, AckMessage, \
    RegisterPeerMessage, ReadyToReceiveMessage
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

    def _handle_messages(self):
        for message in self._background_listener.receive_messages():
            if isinstance(message, PongMessage):
                peer = Peer(connection_info=message.connection_info)
                self._register_peer(peer)
            elif isinstance(message, AckMessage):
                wrapped_message = message.wrapped_message.__root__
                if isinstance(wrapped_message, RegisterPeerMessage):
                    self._send_pong_message(wrapped_message.peer)
                    self._send_ready_to_receive_message(wrapped_message.peer)
                else:
                    print("Unknown wrapped message in ack in _handle_messages", wrapped_message)
            elif isinstance(message, ReadyToReceiveMessage):
                peer = Peer(connection_info=message.connection_info)
                self._add_peer(peer)
            else:
                print("Unknown message in _handle_messages", message)

    def _send_pong_message(self, peer: Peer):
        pong_message = PongMessage(connection_info=self.my_connection_info)
        self._peer_sockets[peer].send_socket.send(serialize_message(pong_message))

    def _send_ready_to_receive_message(self, peer: Peer):
        message = ReadyToReceiveMessage(connection_info=self.my_connection_info)
        self._peer_sockets[peer].send_socket.send(serialize_message(message))

    def wait_for_peers(self, timeout_in_seconds: Optional[int] = None) -> bool:
        start_time_ns = time.monotonic_ns()
        while not self.are_all_peers_connected() and not self._is_timeout(start_time_ns, timeout_in_seconds):
            pass
        return self.are_all_peers_connected()

    def _is_timeout(self, start_time_ns: int, timeout_in_seconds: Optional[int]):
        return timeout_in_seconds is not None and (time.monotonic_ns() - start_time_ns) > timeout_in_seconds * 10 ** 9

    def peers(self, timeout_in_seconds: Optional[int] = None) -> Optional[List[Peer]]:
        self.wait_for_peers(timeout_in_seconds)
        if self.are_all_peers_connected():
            return list(self._sorted_peers)
        else:
            return None

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

    def _add_peer(self, peer: Peer):
        self._sorted_peers.add(peer)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def are_all_peers_connected(self) -> bool:
        self._handle_messages()
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
