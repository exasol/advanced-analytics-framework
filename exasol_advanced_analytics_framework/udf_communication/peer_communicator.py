import enum
import threading
import urllib.parse
from typing import Optional, Dict, List, Iterator, Set, cast

import zmq
from sortedcontainers import SortedSet
from zmq import Frame

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import RegisterPeerMessage, StopMessage, Message, \
    PongMessage, PayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


def get_peer_receive_socket_name(peer: Peer):
    quoted_ip_address = urllib.parse.quote_plus(peer.connection_info.ipaddress.ip_address)
    quoted_port = urllib.parse.quote_plus(str(peer.connection_info.port))
    quoted_group_identifier = peer.connection_info.group_identifier
    return f"inproc://peer/{quoted_group_identifier}/{quoted_ip_address}/{quoted_port}"


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
        self.send_socket.connect(f"tcp://{peer.connection_info.ipaddress}:{peer.connection_info.port}")


class BackgroundListenerRun:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        STOPPED = enum.auto()

    def __init__(self, context: zmq.Context, control_socket_address: str, poll_timeout_in_seconds=1):
        self._poll_timeout_in_seconds = poll_timeout_in_seconds
        self._control_socket_address = control_socket_address
        self._context = context
        self._status = BackgroundListenerRun.Status.RUNNING

    def run(self):
        self._pipe_socket_for_peers: Dict[Peer, zmq.Socket] = {}
        self._create_control_socket()
        port = self._create_listener_socket()
        self._set_my_connection_info(port)
        self._create_poller()
        self._run_message_loop()

    def _create_listener_socket(self):
        self._listener_socket = self._context.socket(zmq.ROUTER)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_control_socket(self):
        self._control_socket = self._context.socket(zmq.PAIR)
        self._control_socket.connect(self._control_socket_address)

    def _run_message_loop(self):
        while self._status == BackgroundListenerRun.Status.RUNNING:
            socks = dict(self.poller.poll(timeout=self._poll_timeout_in_seconds * 1000))
            if self._control_socket in socks and socks[self._control_socket] == zmq.POLLIN:
                message = self._control_socket.recv()
                self._status = self._handle_control_message(message)
            if self._listener_socket in socks and socks[self._listener_socket] == zmq.POLLIN:
                message = self._listener_socket.recv_multipart(copy=False)
                self._handle_listener_message(message)

    def _create_poller(self):
        self.poller = zmq.Poller()
        self.poller.register(self._control_socket, zmq.POLLIN)
        self.poller.register(self._listener_socket, zmq.POLLIN)

    def _handle_control_message(self, message: bytes) -> Status:
        try:
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, StopMessage):
                return BackgroundListenerRun.Status.STOPPED
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                peer = specific_message_obj.peer
                self._add_pipe_socket_for_peer(peer)
            else:
                # ignore and log
                pass
        except Exception as e:
            # ignore and log
            pass
        return BackgroundListenerRun.Status.RUNNING

    def _handle_listener_message(self, message: List[Frame]):
        try:
            message_obj: Message = deserialize_message(message[2].bytes, Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, PongMessage):
                self._control_socket.send(message[2])
            elif isinstance(specific_message_obj, PayloadMessage):
                peer = Peer(connection_info=specific_message_obj.connection_info)
                self._pipe_socket_for_peers[peer].send(message[3])
            else:
                # ignore and log
                pass
        except Exception as e:
            # ignore and log
            pass

    def _set_my_connection_info(self, port: int):
        self._control_socket.send(port)

    def _add_pipe_socket_for_peer(self, peer: Peer):
        if peer in self._pipe_socket_for_peers:
            pass
            # ignore and log
        pipe_socket = zmq.Socket(zmq.DEALER)
        pipe_socket.connect(get_peer_receive_socket_name(peer))
        self._pipe_socket_for_peers[peer] = pipe_socket


class BackgroundListener:

    def __init__(self, context: zmq.Context, listen_ip: IPAddress, group_identifier: str):
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._control_socket = context.socket(zmq.PAIR)
        control_socket_address = f"inproc://BackgroundListener{id(self)}"
        self._control_socket.bind(control_socket_address)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._my_connection_info_ready_event = threading.Event()
        self._background_listener_run = BackgroundListenerRun(context, control_socket_address)
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.start()
        self._set_my_connection_info()

    def _set_my_connection_info(self):
        port = self._control_socket.recv()
        self._my_connection_info = ConnectionInfo(
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier
        )

    @property
    def my_connection_info(self) -> ConnectionInfo:
        self._my_connection_info_ready_event.wait()
        return self._my_connection_info

    def stop(self):
        stop_message = StopMessage()
        self._control_socket.send(serialize_message(stop_message))

    def register_peer(self, peer: Peer):
        register_message = RegisterPeerMessage(peer=peer)
        self._control_socket.send(serialize_message(register_message))

    def receive_pong_messages(self) -> Iterator[PongMessage]:
        while self._control_socket.poll(flags=zmq.POLLIN, timeout=0) != 0:
            message = self._control_socket.recv()
            message_obj: Message = deserialize_message(message, Message)
            if isinstance(message_obj, PongMessage):
                yield PongMessage
            else:
                # ignore and log
                pass


class PeerCommunicator:

    def __init__(self, number_of_peers: int, listen_ip: IPAddress, group_identifier: str):
        self._number_of_peers = number_of_peers
        self._context = zmq.Context()
        self._background_listener = BackgroundListener(context=self._context,
                                                       listen_ip=listen_ip,
                                                       group_identifier=group_identifier)
        self._my_connection_info = self._background_listener.my_connection_info
        self._peer_sockets: Dict[Peer, PeerSockets] = {}
        self._sorted_peers: Set[Peer] = cast(Set[Peer], SortedSet())

    def _update_peers(self):
        for pong_message in self._background_listener.receive_pong_messages():
            self.add_peer(pong_message.connection_info)

    def peers(self) -> List[Peer]:
        assert self.are_all_peers_connected()
        return list(self._sorted_peers)

    def add_peer(self, peer_connection_info: ConnectionInfo):
        if (peer_connection_info.ipaddress == self._my_connection_info
                and peer_connection_info.group_identifier == self._my_connection_info.group_identifier
                and peer_connection_info.port != self._my_connection_info.port):
            peer = Peer(connection_info=peer_connection_info)
            self._add_peer(peer)

    def _add_peer(self, peer: Peer):
        if peer not in self._peer_sockets:
            peer_sockets = PeerSockets(self._context, peer)
            self._background_listener.register_peer(peer)
            self._peer_sockets[peer] = peer_sockets
            self._sorted_peers.add(peer)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def are_all_peers_connected(self) -> bool:
        self._update_peers()
        return len(self._peer_sockets) < self._number_of_peers - 1

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
