import time
from typing import Optional, Dict, List, Set, cast

import structlog
import zmq
from sortedcontainers import SortedSet
from structlog.types import FilteringBoundLogger
from zmq import PollEvent

from exasol_advanced_analytics_framework.udf_communication.background_listener import BackgroundListener
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.logger_thread import LoggerThread, LazyValue
from exasol_advanced_analytics_framework.udf_communication.messages import PongMessage, PayloadMessage, AckMessage, \
    RegisterPeerMessage, ReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_sockets import PeerSockets
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message

LOGGER: FilteringBoundLogger = structlog.getLogger()


def key_for_peer(peer: Peer):
    return peer.connection_info.ipaddress.ip_address + "_" + str(peer.connection_info.port.port)


class PeerCommunicator:

    def __init__(self, name: str, number_of_peers: int, listen_ip: IPAddress, group_identifier: str):
        self._name = name
        self._log_info = dict(module_name=__name__,
                              clazz=self.__class__.__name__,
                              name=self._name,
                              group_identifier=group_identifier)
        self._logger = LOGGER.bind(**self._log_info)
        self._send_socket = {}
        self._number_of_peers = number_of_peers
        self._context = zmq.Context()
        self._logger_thread = LoggerThread()
        self._background_listener = BackgroundListener(
            name=self._name,
            context=self._context,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            logger_thread=self._logger_thread)
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
                    if not wrapped_message.peer in self._send_socket:
                        self._send_socket[wrapped_message.peer] = self._peer_sockets[
                            wrapped_message.peer].create_send_socket()
                    while True:
                        event_mask = self._send_socket[wrapped_message.peer].poll(timeout=None, flags=PollEvent.POLLOUT)
                        if event_mask == PollEvent.POLLOUT:
                            break
                        elif event_mask == PollEvent.POLLERR:
                            raise Exception("Got POLLERR for _send_socket")
                    self._send_pong_message(self._send_socket[wrapped_message.peer])
                    self._send_ready_to_receive_message(self._send_socket[wrapped_message.peer], wrapped_message.peer)
                else:
                    print("Unknown wrapped message in ack in _handle_messages", wrapped_message)
            elif isinstance(message, ReadyToReceiveMessage):
                peer = Peer(connection_info=message.connection_info)
                self._add_peer(peer)
            else:
                print("Unknown message in _handle_messages", message)

    def _send_pong_message(self, send_socket: zmq.Socket):
        message = PongMessage(connection_info=self.my_connection_info)
        log_info = dict(message=LazyValue(message.dict), location="_send_pong_message", **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        send_socket.send(serialize_message(message))
        self._logger_thread.log("send", before=False, **log_info)

    def _send_ready_to_receive_message(self, send_socket: zmq.Socket, peer: Peer):
        message = ReadyToReceiveMessage(connection_info=self.my_connection_info)
        log_info = dict(message=LazyValue(message.dict), location="_send_pong_message", **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        send_socket.send(serialize_message(message))
        self._logger_thread.log("send", before=False, **log_info)

    def wait_for_peers(self, timeout_in_seconds: Optional[int] = None) -> bool:
        start_time_ns = time.monotonic_ns()
        while True:
            if self.are_all_peers_connected():  # or self._is_timeout(start_time_ns, timeout_in_seconds):
                break
        return self.are_all_peers_connected()

    def _is_timeout(self, start_time_ns: int, timeout_in_seconds: Optional[int]):
        if timeout_in_seconds is None:
            return True
        else:
            time_difference_ns = time.monotonic_ns() - start_time_ns
            timeout_in_ns = timeout_in_seconds * 10 ** 9
            return time_difference_ns > timeout_in_ns

    def peers(self, timeout_in_seconds: Optional[int] = None) -> Optional[List[Peer]]:
        self.wait_for_peers(timeout_in_seconds)
        if self.are_all_peers_connected():
            return list(self._sorted_peers)
        else:
            return None

    def register_peer(self, peer_connection_info: ConnectionInfo):
        self._handle_messages()
        if (peer_connection_info.group_identifier == self._my_connection_info.group_identifier
                and peer_connection_info != self._my_connection_info):
            peer = Peer(connection_info=peer_connection_info)
            self._register_peer(peer)
            self._handle_messages()

    def _register_peer(self, peer):
        if peer not in self._peer_sockets:
            peer_sockets = PeerSockets(self._context, peer)
            self._peer_sockets[peer] = peer_sockets
            self._background_listener.register_peer(peer)

    def _add_peer(self, peer: Peer):
        self._sorted_peers.add(peer)
        self._logger_thread.log("_add_peer", peer=peer.dict(), **self._log_info)

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
        with self._peer_sockets[peer].create_send_socket() as send_socket:
            send_socket.send_multipart([serialize_message(payload_message), message])

    def recv(self, peer: Peer, timeout: Optional[int] = None) -> bytes:
        assert self.are_all_peers_connected()
        receive_socket = self._peer_sockets[peer].receive_socket
        if receive_socket.poll(flags=zmq.POLLIN, timeout=timeout) != 0:
            return receive_socket.recv()

    def close(self):
        self._logger.info("CLOSE")
        if self._background_listener is not None:
            self._background_listener.close()
            self._background_listener = None
        if self._logger_thread is not None:
            self._logger_thread.print()
            self._logger_thread = None
        for socket in self._peer_sockets.values():
            socket.receive_socket.close()
        for socket in self._send_socket.values():
            socket.close()

    def __del__(self):
        self.close()
