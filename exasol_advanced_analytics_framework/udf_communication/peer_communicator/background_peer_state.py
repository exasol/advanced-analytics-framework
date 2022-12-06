import time
from typing import Optional, List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message, WeAreReadyToReceiveMessage, \
    AreYouReadyToReceiveMessage, PeerIsReadyToReceiveMessage, AckReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketFactory, \
    SocketType, Socket, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class Clock():
    def get_current_timestamp_in_ms(self) -> int:
        current_timestamp_in_ms = time.monotonic_ns() // 10 ** 6
        return current_timestamp_in_ms


class Sender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 clock: Clock,
                 reminder_timeout_in_ms: float):
        self._clock = clock
        self._my_connection_info = my_connection_info
        self._reminder_timeout_in_ms = reminder_timeout_in_ms
        self._peer = peer
        self._socket_factory = socket_factory
        self._last_send_timestamp_in_ms: Optional[int] = None
        self._logger = LOGGER.bind(
            module_name=__name__,
            clazz=self.__class__.__name__,
            peer=self._peer,
            my_connection_info=self._my_connection_info,
        )

    def _is_time(self, last_timestamp_in_ms: int):
        current_timestamp_in_ms = self._clock.get_current_timestamp_in_ms()
        diff = current_timestamp_in_ms - last_timestamp_in_ms
        return diff > self._reminder_timeout_in_ms

    def _create_send_socket(self) -> Socket:
        send_socket: Socket = self._socket_factory.create_socket(SocketType.DEALER)
        send_socket.connect(
            f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
        return send_socket

    def _send(self, message: Message):
        send_socket: Optional[Socket] = None
        try:
            send_socket = self._create_send_socket()
            serialized_message = serialize_message(message.__root__)
            send_socket.send(serialized_message)
        finally:
            if send_socket is not None:
                send_socket.close(linger=10)


class WeAreReadyMessageSender(Sender):
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 clock: Clock,
                 reminder_timeout_in_ms: float):
        super().__init__(my_connection_info,
                         socket_factory,
                         peer,
                         clock,
                         reminder_timeout_in_ms)
        self._ack_received = False

    def received_ack(self):
        self._ack_received = True

    def send_if_necessary(self, force: bool):
        should_send_we_are_ready_to_receive = self._should_we_send() or force
        if should_send_we_are_ready_to_receive:
            message = Message(__root__=WeAreReadyToReceiveMessage(source=self._my_connection_info))
            self._send(message)
            self._last_send_timestamp_in_ms = self._clock.get_current_timestamp_in_ms()

    def _should_we_send(self):
        is_time = (self._last_send_timestamp_in_ms is None or self._is_time(
            self._last_send_timestamp_in_ms))
        is_enabled = not self._ack_received
        result = is_time and is_enabled
        return result


class AreYouReadySender(Sender):
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 clock: Clock,
                 reminder_timeout_in_ms: float):
        super().__init__(my_connection_info,
                         socket_factory,
                         peer,
                         clock,
                         reminder_timeout_in_ms)
        self._received_we_are_ready_to_receive = False

    def received_we_are_ready_to_receive(self):
        self._received_we_are_ready_to_receive = True

    def send_if_necessary(self):
        should_send_are_you_ready_to_receive = self._should_we_send()
        if should_send_are_you_ready_to_receive:
            message = Message(__root__=AreYouReadyToReceiveMessage(source=self._my_connection_info))
            self._send(message)
            self._last_send_timestamp_in_ms = self._clock.get_current_timestamp_in_ms()

    def _should_we_send(self):
        is_time = (self._last_send_timestamp_in_ms is None
                   or self._is_time(self._last_send_timestamp_in_ms))
        is_enabled = not self._received_we_are_ready_to_receive
        result = is_time and is_enabled
        return result


class PeerIsReadySender(Sender):
    def __init__(self,
                 out_control_socket: Socket,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 clock: Clock,
                 reminder_timeout_in_ms: float,
                 countdown_max: int = 3):
        super().__init__(my_connection_info,
                         socket_factory,
                         peer,
                         clock,
                         reminder_timeout_in_ms)
        self._out_control_socket = out_control_socket
        self._countdown_max = countdown_max
        self.reset_countdown()
        self._finished = False

    def reset_countdown(self):
        self._countdown = self._countdown_max

    def send_if_necessary(self):
        should_send_peer_is_ready_to_frontend = self._should_send_peer_is_ready_to_frontend()
        if should_send_peer_is_ready_to_frontend:
            self._last_send_timestamp_in_ms = self._clock.get_current_timestamp_in_ms()
            if self._countdown == 0:
                self._finished = True
                self._send_peer_is_ready_to_frontend()
            else:
                self._countdown -= 1

    def _should_send_peer_is_ready_to_frontend(self):
        is_time = (self._last_send_timestamp_in_ms is None or self._is_time(self._last_send_timestamp_in_ms))
        return is_time and not self._finished

    def _send_peer_is_ready_to_frontend(self):
        message = PeerIsReadyToReceiveMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)


class BackgroundPeerState(Sender):

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 out_control_socket: Socket,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 clock: Clock = Clock(),
                 reminder_timeout_in_ms: float = 100,
                 countdown_max: int = 3):
        super().__init__(my_connection_info,
                         socket_factory,
                         peer,
                         clock,
                         reminder_timeout_in_ms)
        self._countdown_max = countdown_max
        self._out_control_socket = out_control_socket
        self._create_receive_socket()
        self._are_you_ready_to_receive_sender = AreYouReadySender(
            my_connection_info=self._my_connection_info,
            socket_factory=self._socket_factory,
            peer=self._peer,
            clock=self._clock,
            reminder_timeout_in_ms=self._reminder_timeout_in_ms
        )
        self._we_are_ready_to_receive_sender: Optional[WeAreReadyMessageSender] = None
        self._peer_is_ready_sender: Optional[PeerIsReadySender] = None
        self._are_you_ready_to_receive_sender.send_if_necessary()

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    def resend_if_necessary(self):
        self._are_you_ready_to_receive_sender.send_if_necessary()
        if self._we_are_ready_to_receive_sender is not None:
            self._we_are_ready_to_receive_sender.send_if_necessary(force=False)
        if self._peer_is_ready_sender is not None:
            self._peer_is_ready_sender.send_if_necessary()

    def _send_ack(self):
        message = Message(__root__=AckReadyToReceiveMessage(source=self._my_connection_info))
        self._send(message)

    def received_we_are_ready_to_receive(self):
        self._are_you_ready_to_receive_sender.received_we_are_ready_to_receive()
        self._send_ack()
        if self._peer_is_ready_sender is not None:
            self._peer_is_ready_sender.reset_countdown()

    def received_ack(self):
        self._we_are_ready_to_receive_sender.received_ack()
        if self._peer_is_ready_sender is None:
            self._peer_is_ready_sender = PeerIsReadySender(
                out_control_socket=self._out_control_socket,
                my_connection_info=self._my_connection_info,
                socket_factory=self._socket_factory,
                peer=self._peer,
                clock=self._clock,
                reminder_timeout_in_ms=self._reminder_timeout_in_ms,
                countdown_max=self._countdown_max
            )
        self._peer_is_ready_sender.send_if_necessary()

    def received_are_you_ready_to_receive(self):
        if self._we_are_ready_to_receive_sender is None:
            self._we_are_ready_to_receive_sender = WeAreReadyMessageSender(
                my_connection_info=self._my_connection_info,
                socket_factory=self._socket_factory,
                peer=self._peer,
                clock=self._clock,
                reminder_timeout_in_ms=self._reminder_timeout_in_ms
            )
        self._we_are_ready_to_receive_sender.send_if_necessary(force=True)
        if self._peer_is_ready_sender is not None:
            self._peer_is_ready_sender.reset_countdown()

    def forward_payload(self, frames: List[Frame]):
        self._receive_socket.send_multipart(frames)

    def close(self):
        self._receive_socket.close(linger=0)
