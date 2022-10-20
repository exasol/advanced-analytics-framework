import time
from typing import Optional, Dict, List, Set, cast

import structlog
import zmq
from sortedcontainers import SortedSet
from structlog.types import FilteringBoundLogger

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
        self._acks: Set[Peer] = set()
        self._sorted_peers: Set[Peer] = set()  # cast(Set[Peer], SortedSet(key=key_for_peer))

    def _handle_messages(self):
        for message in self._background_listener.receive_messages():
            if isinstance(message, PongMessage):
                peer = Peer(connection_info=message.connection_info)
                self._register_peer(peer)
            elif isinstance(message, AckMessage):
                self._handle_ack(message)
            elif isinstance(message, ReadyToReceiveMessage):
                peer = Peer(connection_info=message.connection_info)
                self._create_send_socket(peer)
                self._add_peer(peer)
                self._send_ack_ready_to_receive_message(self._send_socket[peer], message)
            else:
                self._logger.error(
                    "Unknown message in ack",
                    location="_handle_messages",
                    message=message.dict())

    def _resend(self):
        for peer, send_socket in self._send_socket.items():
            if not peer in self._sorted_peers:
                # print("before pong", self._name)
                self._send_pong_message(send_socket)
                # print("before ready", self._name)
                self._send_ready_to_receive_message(send_socket)
                # print("after ready", self._name)
            else:
                self._send_ack_ready_to_receive_message(self._send_socket[peer],
                                                        ReadyToReceiveMessage(connection_info=peer.connection_info))

    def _handle_ack(self, message: AckMessage):
        wrapped_message = message.wrapped_message.__root__
        if isinstance(wrapped_message, RegisterPeerMessage):
            self._handle_ack_for_register_peer_message(wrapped_message)
        if isinstance(wrapped_message, ReadyToReceiveMessage):
            peer = Peer(connection_info=message.connection_info)
            if peer not in self._acks:
                self._acks.add(peer)
        else:
            self._logger.error(
                "Unknown wrapped message in ack",
                location="_handle_ack",
                message=wrapped_message.dict())

    def _handle_ack_for_register_peer_message(self, message: RegisterPeerMessage):
        peer = message.peer
        self._create_send_socket(peer)
        self._send_pong_message(self._send_socket[peer])
        self._send_ready_to_receive_message(self._send_socket[peer])

    def _create_send_socket(self, peer):
        if not peer in self._send_socket:
            self._send_socket[peer] = self._peer_sockets[peer].create_send_socket()

    def _send_pong_message(self, send_socket: zmq.Socket):
        message = PongMessage(connection_info=self.my_connection_info)
        log_info = dict(message=LazyValue(message.dict), location="_send_pong_message", **self._log_info)
        if send_socket.poll(0, flags=zmq.POLLOUT) == zmq.POLLOUT:
            self._logger_thread.log("send", before=True, **log_info)
            send_socket.send(serialize_message(message))
            self._logger_thread.log("send", before=False, **log_info)

    def _send_ready_to_receive_message(self, send_socket: zmq.Socket):
        message = ReadyToReceiveMessage(connection_info=self.my_connection_info)
        log_info = dict(message=LazyValue(message.dict), location="_send_pong_message", **self._log_info)
        if send_socket.poll(0, flags=zmq.POLLOUT) == zmq.POLLOUT:
            self._logger_thread.log("send", before=True, **log_info)
            send_socket.send(serialize_message(message))
            self._logger_thread.log("send", before=False, **log_info)

    def _send_ack_ready_to_receive_message(self, send_socket: zmq.Socket, message: ReadyToReceiveMessage):
        message = AckMessage(connection_info=self.my_connection_info, wrapped_message=message)
        log_info = dict(message=LazyValue(message.dict), location="_send_ack_ready_to_receive_message",
                        **self._log_info)
        if send_socket.poll(0, flags=zmq.POLLOUT) == zmq.POLLOUT:
            self._logger_thread.log("send", before=True, **log_info)
            send_socket.send(serialize_message(message))
            self._logger_thread.log("send", before=False, **log_info)

    def wait_for_peers(self, timeout_in_seconds: Optional[int] = None) -> bool:
        start_time_ns = time.monotonic_ns()
        old_time_difference_ns = 0
        # print("test0", self._name)
        while True:
            new_time_difference_ns = self._get_time_difference(start_time_ns)
            if (new_time_difference_ns - old_time_difference_ns) > 1 * 10 ** 9:
                old_time_difference_ns = new_time_difference_ns

                self._resend()
                # print("test1", self._name,
                #       [p.connection_info.name for p in self._sorted_peers],
                #       [p.connection_info.name for p in self._acks])
                # print("Print", self._name, flush=True)
                #self._logger_thread.print()
            self._handle_messages()
            if self.are_all_peers_connected():  # or self._is_timeout(start_time_ns, timeout_in_seconds):
                break
            time.sleep(0.0001)
        connected = self.are_all_peers_connected()
        # print("test2", self._name,
        #       [p.connection_info.name for p in self._sorted_peers],
        #       [p.connection_info.name for p in self._acks])
        return connected

    def _get_time_difference(self, start_time_ns):
        time_difference_ns = time.monotonic_ns() - start_time_ns
        return time_difference_ns

    # def _is_timeout(self, start_time_ns: int, timeout_in_seconds: Optional[int]):
    #     if timeout_in_seconds is None:
    #         return True
    #     else:
    #
    #         timeout_in_ns = timeout_in_seconds * 10 ** 9
    #         return time_difference_ns > timeout_in_ns

    def peers(self, timeout_in_seconds: Optional[int] = None) -> Optional[List[Peer]]:
        self.wait_for_peers(timeout_in_seconds)
        if self.are_all_peers_connected():
            return sorted(list(self._sorted_peers), key=key_for_peer)
        else:
            return None

    def register_peer(self, peer_connection_info: ConnectionInfo):
        self._handle_messages()
        if (peer_connection_info.group_identifier == self.my_connection_info.group_identifier
                and peer_connection_info != self.my_connection_info):
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
        # self._handle_messages()
        return len(self._sorted_peers) == self._number_of_peers - 1 and len(self._acks) == self._number_of_peers - 1

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
            #print("Print", self._name, flush=True)
            self._logger_thread.print()
            self._logger_thread = None
        for socket in self._peer_sockets.values():
            socket.receive_socket.setsockopt(zmq.LINGER, 0)
            socket.receive_socket.close()
        for socket in self._send_socket.values():
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
        self._context.setsockopt(zmq.LINGER, 0)
        self._context.term()

    def __del__(self):
        self.close()
