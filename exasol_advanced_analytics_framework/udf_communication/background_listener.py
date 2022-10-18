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
    PongMessage, PayloadMessage, AckMessage, MyConnectionInfoMessage, ReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message


class BackgroundListenerRun:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        STOPPED = enum.auto()

    def __init__(self,
                 context: zmq.Context,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 out_control_socket_address: str,
                 in_control_socket_address: str,
                 poll_timeout_in_seconds=1):
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._in_control_socket_address = in_control_socket_address
        self._out_control_socket_address = out_control_socket_address
        self._poll_timeout_in_seconds = poll_timeout_in_seconds
        self._context = context
        self._status = BackgroundListenerRun.Status.RUNNING

    def run(self):
        self._pipe_socket_for_peers: Dict[Peer, zmq.Socket] = {}
        self._create_in_control_socket()
        self._create_out_control_socket()
        port = self._create_listener_socket()
        self._set_my_connection_info(port)
        self._create_poller()
        self._run_message_loop()

    def _create_listener_socket(self):
        self._listener_socket = self._context.socket(zmq.ROUTER)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_in_control_socket(self):
        self._in_control_socket = self._context.socket(zmq.PAIR)
        self._in_control_socket.connect(self._in_control_socket_address)

    def _create_out_control_socket(self):
        self._out_control_socket = self._context.socket(zmq.PAIR)
        self._out_control_socket.connect(self._out_control_socket_address)

    def _run_message_loop(self):
        while self._status == BackgroundListenerRun.Status.RUNNING:
            socks = dict(self.poller.poll(timeout=self._poll_timeout_in_seconds * 1000))
            if self._in_control_socket in socks and socks[self._in_control_socket] == zmq.POLLIN:
                message = self._in_control_socket.recv()
                self._status = self._handle_control_message(message)
            if self._listener_socket in socks and socks[self._listener_socket] == zmq.POLLIN:
                message = self._listener_socket.recv_multipart(copy=False)
                self._handle_listener_message(message)

    def _create_poller(self):
        self.poller = zmq.Poller()
        self.poller.register(self._in_control_socket, zmq.POLLIN)
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
                ack_message = AckMessage(wrapped_message=specific_message_obj)
                self._out_control_socket.send(serialize_message(ack_message))
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
                self._out_control_socket.send(message[1])
            elif isinstance(specific_message_obj, ReadyToReceiveMessage):
                self._out_control_socket.send(message[1])
            elif isinstance(specific_message_obj, PayloadMessage):
                peer = Peer(connection_info=specific_message_obj.connection_info)
                self._pipe_socket_for_peers[peer].send(message[2])
            else:
                print("unknown listener message", specific_message_obj)
        except Exception as e:
            print("Exception while handling listener message", message[0].bytes, message[1].bytes, message[2].bytes)
            traceback.print_exc()

    def _set_my_connection_info(self, port: int):
        my_connection_info = ConnectionInfo(ipaddress=self._listen_ip, port=Port(port=port),
                                            group_identifier=self._group_identifier)
        message = MyConnectionInfoMessage(connection_info=my_connection_info)
        self._out_control_socket.send(serialize_message(message))

    def _add_pipe_socket_for_peer(self, peer: Peer):
        if peer in self._pipe_socket_for_peers:
            return
        pipe_socket = self._context.socket(zmq.DEALER)
        pipe_socket.connect(get_peer_receive_socket_name(peer))
        self._pipe_socket_for_peers[peer] = pipe_socket


class BackgroundListener:

    def __init__(self, context: zmq.Context, listen_ip: IPAddress, group_identifier: str):
        out_control_socket_address = self._create_out_control_socket(context)
        in_control_socket_address = self._create_in_control_socket(context)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._background_listener_run = BackgroundListenerRun(context,
                                                              listen_ip,
                                                              group_identifier,
                                                              out_control_socket_address,
                                                              in_control_socket_address)
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.setDaemon(True)
        self._thread.start()
        self._set_my_connection_info()

    def _create_in_control_socket(self, context) -> str:
        self._in_control_socket = context.socket(zmq.PAIR)
        in_control_socket_address = f"inproc://BackgroundListener_in_control_socket{id(self)}"
        self._in_control_socket.bind(in_control_socket_address)
        return in_control_socket_address

    def _create_out_control_socket(self, context) -> str:
        self._out_control_socket = context.socket(zmq.PAIR)
        out_control_socket_address = f"inproc://BackgroundListener_out_control_socket{id(self)}"
        self._out_control_socket.bind(out_control_socket_address)
        return out_control_socket_address

    def _set_my_connection_info(self):
        message = self._out_control_socket.recv()
        try:
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            assert isinstance(specific_message_obj, MyConnectionInfoMessage)
            self._my_connection_info = specific_message_obj.connection_info
        except Exception as e:
            print("Exception while handling my_connection_info messages", message)
            traceback.print_exc()

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def stop(self):
        stop_message = StopMessage()
        self._in_control_socket.send(serialize_message(stop_message))

    def register_peer(self, peer: Peer):
        register_message = RegisterPeerMessage(peer=peer)
        self._in_control_socket.send(serialize_message(register_message))

    def receive_messages(self) -> Iterator[Message]:
        while self._out_control_socket.poll(flags=zmq.POLLIN, timeout=0) != 0:
            message = self._out_control_socket.recv()
            try:
                message_obj: Message = deserialize_message(message, Message)
                specific_message_obj = message_obj.__root__
                yield specific_message_obj
            except Exception as e:
                print("Exception while handling pong messages", message)
                traceback.print_exc()

    def __del__(self):
        self.stop()
