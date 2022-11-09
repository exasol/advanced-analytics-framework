import contextlib
from typing import Optional, Generator, List

import zmq

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.messages import PayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message


class FrontendPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 context: zmq.Context,
                 peer: Peer):
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._context = context
        self._peer_is_ready = False
        self._create_receive_socket()

    def _create_receive_socket(self):
        self._receive_socket = self._context.socket(zmq.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.connect(receive_socket_address)

    @contextlib.contextmanager
    def _create_send_socket(self) -> Generator[zmq.Socket, None, None]:
        send_socket: zmq.Socket
        with self._context.socket(zmq.DEALER) as send_socket:
            send_socket.connect(
                f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
            yield send_socket

    def received_peer_is_ready_to_receive(self):
        self._peer_is_ready = True

    @property
    def peer_is_ready(self) -> bool:
        return self._peer_is_ready

    @property
    def receive_socket(self) -> zmq.Socket:
        return self._receive_socket

    def send(self, payload: List[bytes]):
        send_socket: zmq.Socket
        with self._create_send_socket() as send_socket:
            message = PayloadMessage(source=self._my_connection_info)
            serialized_message = serialize_message(message)
            send_socket.send_multipart([serialized_message] + payload)

    def recv(self, timeout_in_milliseconds: Optional[int] = None) -> List[bytes]:
        if self._receive_socket.poll(flags=zmq.POLLIN, timeout=timeout_in_milliseconds) != 0:
            return self._receive_socket.recv_multipart()

    def close(self):
        self._receive_socket.close(linger=0)
