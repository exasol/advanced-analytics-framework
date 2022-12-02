import enum
from typing import Dict, List, Optional

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    PayloadMessage, MyConnectionInfoMessage, SynchronizeConnectionMessage, AcknowledgeConnectionMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.send_socket_factory import \
    SendSocketFactory
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    SocketType, Socket, PollerFlag, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundListenerThread:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        STOPPED = enum.auto()

    def __init__(self,
                 name: str,
                 socket_factory: SocketFactory,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 leader: bool,
                 forward: bool,
                 out_control_socket_address: str,
                 in_control_socket_address: str,
                 clock: Clock,
                 poll_timeout_in_ms: int,
                 synchronize_timeout_in_ms: int,
                 abort_timeout_in_ms: int,
                 peer_is_ready_wait_time_in_ms: int,
                 send_socket_linger_time_in_ms: int,
                 trace_logging: bool):
        self._register_peer_connection: Optional[RegisterPeerConnection] = None
        self._forward = forward
        self._leader = leader
        self._send_socket_linger_time_in_ms = send_socket_linger_time_in_ms
        self._trace_logging = trace_logging
        self._clock = clock
        self._peer_is_ready_wait_time_in_ms = peer_is_ready_wait_time_in_ms
        self._abort_timeout_in_ms = abort_timeout_in_ms
        self._synchronize_timeout_in_ms = synchronize_timeout_in_ms
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=group_identifier,
            leader=self._leader,
            forward=self._forward
        )
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._in_control_socket_address = in_control_socket_address
        self._out_control_socket_address = out_control_socket_address
        self._poll_timeout_in_ms = poll_timeout_in_ms
        self._socket_factory = socket_factory
        self._status = BackgroundListenerThread.Status.RUNNING
        self._peer_state: Dict[Peer, BackgroundPeerState] = {}

    def run(self):
        self._create_in_control_socket()
        self._create_out_control_socket()
        port = self._create_listener_socket()
        self._set_my_connection_info(port)
        self._create_poller()
        try:
            self._run_message_loop()
        finally:
            self._close()

    def _close(self):
        self._logger.info("start")
        if self._register_peer_connection is not None:
            self._register_peer_connection.close()
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        for peer_state in self._peer_state.values():
            peer_state.close()
        self._listener_socket.close(linger=0)
        self._logger.info("end")

    def _create_listener_socket(self):
        self._listener_socket: Socket = self._socket_factory.create_socket(SocketType.ROUTER)
        self._listener_socket.set_identity(self._name)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_in_control_socket(self):
        self._in_control_socket: Socket = self._socket_factory.create_socket(SocketType.PAIR)
        self._in_control_socket.connect(self._in_control_socket_address)

    def _create_out_control_socket(self):
        self._out_control_socket: Socket = self._socket_factory.create_socket(SocketType.PAIR)
        self._out_control_socket.connect(self._out_control_socket_address)

    def _create_poller(self):
        self.poller = self._socket_factory.create_poller()
        self.poller.register(self._in_control_socket, flags=PollerFlag.POLLIN)
        self.poller.register(self._listener_socket, flags=PollerFlag.POLLIN)

    def _run_message_loop(self):
        try:
            while self._status == BackgroundListenerThread.Status.RUNNING:
                poll = self.poller.poll(timeout_in_ms=self._poll_timeout_in_ms)
                if self._in_control_socket in poll and PollerFlag.POLLIN in poll[self._in_control_socket]:
                    message = self._in_control_socket.receive()
                    self._status = self._handle_control_message(message)
                if self._listener_socket in poll and PollerFlag.POLLIN in poll[self._listener_socket]:
                    message = self._listener_socket.receive_multipart()
                    self._handle_listener_message(message)
                if self._status == BackgroundListenerThread.Status.RUNNING:
                    for peer_state in self._peer_state.values():
                        peer_state.resend_if_necessary()
        except Exception as e:
            self._logger.exception("Exception in message loop")


    def _handle_control_message(self, message: bytes) -> Status:
        try:
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, StopMessage):
                return BackgroundListenerThread.Status.STOPPED
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                if self._forward and self._leader or not self._forward:
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    self._logger.error("RegisterPeerMessage message not allowed", message=specific_message_obj.dict())
            else:
                self._logger.error("Unknown message type", message=specific_message_obj.dict())
        except Exception as e:
            self._logger.exception("Exception during handling message", message=message)
        return BackgroundListenerThread.Status.RUNNING

    def _add_peer(self, peer: Peer, forward: bool = False):
        if peer.connection_info.group_identifier != self._my_connection_info.group_identifier:
            self._logger.error("Peer belongs to a different group",
                               my_connection_info=self._my_connection_info.dict(),
                               peer=peer.dict())
            raise ValueError("Peer belongs to a different group")
        if peer not in self._peer_state:
            register_peer_connection: Optional[RegisterPeerConnection] = None
            if forward:
                register_peer_connection = self._register_peer_connection
            self._peer_state[peer] = BackgroundPeerState.create(
                my_connection_info=self._my_connection_info,
                out_control_socket=self._out_control_socket,
                socket_factory=self._socket_factory,
                peer=peer,
                register_peer_connection=register_peer_connection,
                clock=self._clock,
                peer_is_ready_wait_time_in_ms=self._peer_is_ready_wait_time_in_ms,
                abort_timeout_in_ms=self._abort_timeout_in_ms,
                synchronize_timeout_in_ms=self._synchronize_timeout_in_ms,
                send_socket_linger_time_in_ms=self._send_socket_linger_time_in_ms
            )

    def _handle_listener_message(self, message: List[Frame]):
        logger = self._logger.bind(
            sender=message[0].to_bytes()
        )
        message_content_bytes = message[1].to_bytes()
        try:
            message_obj: Message = deserialize_message(message_content_bytes, Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, SynchronizeConnectionMessage):
                self._handle_synchronize_connection(specific_message_obj)
            elif isinstance(specific_message_obj, AcknowledgeConnectionMessage):
                self._handle_acknowledge_connection(specific_message_obj)
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                if not self._leader and self._forward:
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    logger.error("RegisterPeerMessage message not allowed", message=specific_message_obj.dict())
            elif isinstance(specific_message_obj, PayloadMessage):
                self._handle_payload_message(specific_message_obj, message)
            else:
                logger.error("Unknown message type", message=specific_message_obj.dict())
        except Exception as e:
            logger.exception("Exception during handling message", message_content=message_content_bytes)

    def _handle_payload_message(self, message: PayloadMessage, frames: List[Frame]):
        peer = Peer(connection_info=message.source)
        self._peer_state[peer].forward_payload(frames[2:])

    def _handle_synchronize_connection(self, message: SynchronizeConnectionMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_synchronize_connection()

    def _handle_acknowledge_connection(self, message: AcknowledgeConnectionMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_acknowledge_connection()

    def _set_my_connection_info(self, port: int):
        self._my_connection_info = ConnectionInfo(
            name=self._name,
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier)
        message = MyConnectionInfoMessage(my_connection_info=self._my_connection_info)
        self._out_control_socket.send(serialize_message(message))

    def _handle_register_peer_message(self, message: RegisterPeerMessage):
        if self._forward:
            if self._register_peer_connection is None:
                self._create_register_peer_connection(message)
                self._add_peer(message.peer)
            else:
                self._add_peer(message.peer, forward=True)
        else:
            self._add_peer(message.peer)

    def _create_register_peer_connection(self, message: RegisterPeerMessage):
        successor_send_socket_factory = SendSocketFactory(
            my_connection_info=self._my_connection_info,
            peer=message.peer,
            socket_factory=self._socket_factory
        )
        if message.source is not None:
            predecessor_send_socket_factory = SendSocketFactory(
                my_connection_info=self._my_connection_info,
                peer=message.source,
                socket_factory=self._socket_factory
            )
        else:
            predecessor_send_socket_factory = None
        self._register_peer_connection = RegisterPeerConnection(
            predecessor_send_socket_factory=predecessor_send_socket_factory,
            successor_send_socket_factory=successor_send_socket_factory,
            my_connection_info=self._my_connection_info
        )
