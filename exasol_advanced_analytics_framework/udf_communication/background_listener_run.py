import enum
from typing import Dict, List

import structlog
import zmq
from structlog.types import FilteringBoundLogger
from zmq import Frame

from exasol_advanced_analytics_framework.udf_communication.background_peer_state import BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.logger_thread import LoggerThread, LazyValue
from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    WeAreReadyToReceiveMessage, PayloadMessage, MyConnectionInfoMessage, AreYouReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundListenerRun:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        STOPPED = enum.auto()

    def __init__(self,
                 name: str,
                 context: zmq.Context,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 out_control_socket_address: str,
                 in_control_socket_address: str,
                 logger_thread: LoggerThread,
                 poll_timeout_in_seconds: int = 1,
                 reminder_timeout_in_seconds: float = 1):
        self._wait_time_between_reminder_in_seconds = reminder_timeout_in_seconds
        self._name = name
        self._log_info = dict(module_name=__name__,
                              clazz=self.__class__.__name__,
                              name=self._name,
                              group_identifier=group_identifier)
        self._logger = LOGGER.bind(**self._log_info)
        self._logger_thread = logger_thread
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._in_control_socket_address = in_control_socket_address
        self._out_control_socket_address = out_control_socket_address
        self._poll_timeout_in_seconds = poll_timeout_in_seconds
        self._context = context
        self._status = BackgroundListenerRun.Status.RUNNING

    def run(self):
        self._peer_state: Dict[Peer, BackgroundPeerState] = {}
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
        log_info = dict(location="close", **self._log_info)
        self._logger.info("start", **log_info)
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        for peer_state in self._peer_state.values():
            peer_state.close()
        self._listener_socket.close(linger=0)
        self._logger.info("end", **log_info)

    def _create_listener_socket(self):
        self._listener_socket: zmq.Socket = self._context.socket(zmq.ROUTER)
        self._listener_socket.set_string(zmq.IDENTITY, self._name)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_in_control_socket(self):
        self._in_control_socket: zmq.Socket = self._context.socket(zmq.PAIR)
        self._in_control_socket.connect(self._in_control_socket_address)

    def _create_out_control_socket(self):
        self._out_control_socket: zmq.Socket = self._context.socket(zmq.PAIR)
        self._out_control_socket.connect(self._out_control_socket_address)

    def _create_poller(self):
        self.poller = zmq.Poller()
        self.poller.register(self._in_control_socket, zmq.POLLIN)
        self.poller.register(self._listener_socket, zmq.POLLIN)

    def _run_message_loop(self):
        log = self._logger.bind(location="_run_message_loop")
        try:
            while self._status == BackgroundListenerRun.Status.RUNNING:
                socks = dict(self.poller.poll(timeout=self._poll_timeout_in_seconds * 1000))
                if self._in_control_socket in socks and socks[self._in_control_socket] == zmq.POLLIN:
                    message = self._in_control_socket.recv()
                    self._status = self._handle_control_message(message)
                elif self._listener_socket in socks and socks[self._listener_socket] == zmq.POLLIN:
                    message = self._listener_socket.recv_multipart(copy=False)
                    self._handle_listener_message(message)
                elif len(socks) != 0:
                    log.error("Sockets unhandled event", socks=socks)
        except Exception as e:
            log.exception("Exception")

    def _handle_control_message(self, message: bytes) -> Status:
        log_info = dict(location="_handle_control_message", **self._log_info)
        try:
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            self._logger_thread.log("recieved_message", message=LazyValue(specific_message_obj.dict), **log_info)
            if isinstance(specific_message_obj, StopMessage):
                return BackgroundListenerRun.Status.STOPPED
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                self._add_peer(specific_message_obj.peer)
            else:
                self._logger.error(
                    "Unknown message type",
                    message=specific_message_obj.dict(),
                    **log_info)
        except Exception as e:
            self._logger.exception(
                "Could not deserialize message",
                message=message,
                **log_info
            )
        return BackgroundListenerRun.Status.RUNNING

    def _add_peer(self, peer):
        if peer not in self._peer_state:
            self._peer_state[peer] = BackgroundPeerState(
                my_connection_info=self._my_connection_info,
                out_control_socket=self._out_control_socket,
                context=self._context,
                peer=peer,
                reminder_timeout_in_seconds=self._wait_time_between_reminder_in_seconds
            )

    def _handle_listener_message(self, message: List[Frame]):
        log_info = dict(
            location="_handle_listener_message",
            sender=message[1].bytes,
            **self._log_info
        )
        try:
            message_obj: Message = deserialize_message(message[1].bytes, Message)
            specific_message_obj = message_obj.__root__
            self._logger_thread.log("recieved_message", message=LazyValue(specific_message_obj.dict), **log_info)
            if isinstance(specific_message_obj, WeAreReadyToReceiveMessage):
                self._handle_we_are_ready_to_receive(specific_message_obj)
            if isinstance(specific_message_obj, AreYouReadyToReceiveMessage):
                self._handle_are_you_ready_to_receive(specific_message_obj)
            elif isinstance(specific_message_obj, PayloadMessage):
                self._handle_payload_message(specific_message_obj, message)
            else:
                self._logger.error("Unknown message type", message=specific_message_obj.dict(), **log_info)
        except Exception as e:
            self._logger.exception(
                "Could not deserialize message",
                message=message[1].bytes,
                **log_info
            )

    def _handle_payload_message(self, message: PayloadMessage, frames: List[Frame]):
        peer = Peer(connection_info=message.source)
        self._peer_state[peer].forward_payload(frames[2:])

    def _handle_we_are_ready_to_receive(self, message: WeAreReadyToReceiveMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_peer_is_ready_to_receive()

    def _handle_are_you_ready_to_receive(self, message: AreYouReadyToReceiveMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_are_you_ready_to_receive()

    def _set_my_connection_info(self, port: int):
        self._my_connection_info = ConnectionInfo(
            name=self._name,
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier)
        message = MyConnectionInfoMessage(my_connection_info=self._my_connection_info)
        log_info = dict(location="_set_my_connection_info",
                        message=LazyValue(message.dict),
                        **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._out_control_socket.send(serialize_message(message))
        self._logger_thread.log("send", before=False, **log_info)
