import contextlib
import time
from typing import Optional, Generator, List

import zmq
from zmq import Frame

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.messages import Message, WeAreReadyToReceiveMessage, \
    AreYouReadyToReceiveMessage, PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message


class BackgroundPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 out_control_socket: zmq.Socket,
                 context: zmq.Context,
                 peer: Peer,
                 reminder_timeout_in_seconds: float = 1):
        self._out_control_socket = out_control_socket
        self._my_connection_info = my_connection_info
        self._wait_time_between_reminder_in_seconds = reminder_timeout_in_seconds
        self._peer = peer
        self._context = context
        self._peer_can_receive_from_us = False
        self._last_send_ready_to_receive_timestamp_in_seconds: Optional[float] = None
        self._create_receive_socket()
        self._send_we_are_ready_to_receive()

    def _create_receive_socket(self):
        self._receive_socket = self._context.socket(zmq.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    @contextlib.contextmanager
    def _create_send_socket(self) -> Generator[zmq.Socket, None, None]:
        send_socket: zmq.Socket
        with self._context.socket(zmq.DEALER) as send_socket:
            send_socket.connect(
                f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
            yield send_socket

    def _is_time_to_send_are_you_ready_to_receive(self):
        if self._last_send_ready_to_receive_timestamp_in_seconds:
            current_timestamp_in_seconds = time.monotonic()
            diff = current_timestamp_in_seconds - self._last_send_ready_to_receive_timestamp_in_seconds
            if diff > self._wait_time_between_reminder_in_seconds:
                self._last_send_ready_to_receive_timestamp_in_seconds = current_timestamp_in_seconds
                return True
        return False

    def _send_are_you_ready_to_receive_if_necassary(self):
        if not self._peer_can_receive_from_us:
            if self._is_time_to_send_are_you_ready_to_receive():
                message = Message(__root__=AreYouReadyToReceiveMessage(source=self._my_connection_info))
                self._send(message)

    def _send_we_are_ready_to_receive(self):
        message = Message(__root__=WeAreReadyToReceiveMessage(source=self._my_connection_info))
        self._send(message)

    def received_peer_is_ready_to_receive(self):
        self._handle_peer_is_ready_to_receive()

    def received_are_you_ready_to_receive(self):
        self._handle_peer_is_ready_to_receive()
        self._send_we_are_ready_to_receive()

    def _handle_peer_is_ready_to_receive(self):
        if not self._peer_can_receive_from_us:
            self._send_peer_is_ready_to_frontend()
            self._peer_can_receive_from_us = True

    def _send(self, message: Message):
        send_socket: zmq.Socket
        with self._create_send_socket() as send_socket:
            serialized_message = serialize_message(message.__root__)
            send_socket.send(serialized_message)

    def _send_peer_is_ready_to_frontend(self):
        message = PeerIsReadyToReceiveMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def forward_payload(self, frames: List[Frame]):
        self._receive_socket.send_multipart(frames)

    def close(self):
        self._receive_socket.close(linger=0)
