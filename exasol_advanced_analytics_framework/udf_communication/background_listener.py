import enum
import threading
import traceback
from typing import Dict, List, Optional, Iterator

import zmq
from zmq import Frame

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port

from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    PongMessage, PayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message


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
                print("unknown control message", specific_message_obj)
        except Exception as e:
            print("Exception while handling control message", message)
            traceback.print_exc()
        return BackgroundListenerRun.Status.RUNNING

    def _handle_listener_message(self, message: List[Frame]):
        try:
            message_obj: Message = deserialize_message(message[1].bytes, Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, PongMessage):
                self._control_socket.send(message[1])
            elif isinstance(specific_message_obj, PayloadMessage):
                peer = Peer(connection_info=specific_message_obj.connection_info)
                self._pipe_socket_for_peers[peer].send(message[2])
            else:
                print("unknown listener message", specific_message_obj)
        except Exception as e:
            print("Exception while handling listener message", message[0].bytes, message[1].bytes, message[2].bytes)
            traceback.print_exc()

    def _set_my_connection_info(self, port: int):
        self._control_socket.send_string(str(port))

    def _add_pipe_socket_for_peer(self, peer: Peer):
        if peer in self._pipe_socket_for_peers:
            pass
            # ignore and log
        pipe_socket = self._context.socket(zmq.DEALER)
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
        self._background_listener_run = BackgroundListenerRun(context, control_socket_address)
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.setDaemon(True)
        self._thread.start()
        self._set_my_connection_info()

    def _set_my_connection_info(self):
        port = self._control_socket.recv_string()
        self._my_connection_info = ConnectionInfo(
            ipaddress=self._listen_ip,
            port=Port(port=int(port)),
            group_identifier=self._group_identifier
        )

    @property
    def my_connection_info(self) -> ConnectionInfo:
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
            try:
                message_obj: Message = deserialize_message(message, Message)
                specific_message_obj = message_obj.__root__
                if isinstance(specific_message_obj, PongMessage):
                    yield specific_message_obj
                else:
                    print("unknown pong message", message)
            except Exception as e:
                print("Exception while handling pong messages", message)
                traceback.print_exc()

    def __del__(self):
        self.stop()
