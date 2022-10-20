import enum
from typing import Dict, List

import structlog
import zmq
from structlog.types import FilteringBoundLogger
from zmq import Frame

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.logger_thread import LoggerThread, LazyValue
from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    AckMessage, PongMessage, ReadyToReceiveMessage, PayloadMessage, MyConnectionInfoMessage
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
                 poll_timeout_in_seconds=1, ):
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
        self._pipe_socket_for_peers: Dict[Peer, zmq.Socket] = {}
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
        self._out_control_socket.setsockopt(zmq.LINGER, 0)
        self._out_control_socket.close()
        self._in_control_socket.setsockopt(zmq.LINGER, 0)
        self._in_control_socket.close()
        for socket in self._pipe_socket_for_peers.values():
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
        self._listener_socket.setsockopt(zmq.LINGER, 0)
        self._listener_socket.close()
        self._logger.info("end", **log_info)

    def _create_listener_socket(self):
        self._listener_socket = self._context.socket(zmq.ROUTER)
        self._listener_socket.set_string(zmq.IDENTITY, self._name)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_in_control_socket(self):
        self._in_control_socket = self._context.socket(zmq.PAIR)
        self._in_control_socket.connect(self._in_control_socket_address)

    def _create_out_control_socket(self):
        self._out_control_socket = self._context.socket(zmq.PAIR)
        self._out_control_socket.connect(self._out_control_socket_address)

    def _run_message_loop(self):
        log = self._logger.bind(location="_run_message_loop")
        try:
            while self._status == BackgroundListenerRun.Status.RUNNING:
                socks = dict(self.poller.poll(timeout=self._poll_timeout_in_seconds * 1000))
                #print("_run_message_loop", self._name, socks, flush=True)
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

    def _create_poller(self):
        self.poller = zmq.Poller()
        self.poller.register(self._in_control_socket, zmq.POLLIN)
        self.poller.register(self._listener_socket, zmq.POLLIN)

    def _handle_control_message(self, message: bytes) -> Status:
        log_info = dict(location="_handle_control_message", **self._log_info)
        try:
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            self._logger_thread.log("recieved_message", message=LazyValue(specific_message_obj.dict), **log_info)
            if isinstance(specific_message_obj, StopMessage):
                return BackgroundListenerRun.Status.STOPPED
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                peer = specific_message_obj.peer
                self._add_pipe_socket_for_peer(peer)
                self._send_ack_to_control_socket(message_obj)
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

    def _send_ack_to_control_socket(self, message: Message):
        ack_message = AckMessage(wrapped_message=message.__root__)
        log_info = dict(location="_send_ack_to_control_socket",
                        message=LazyValue(ack_message.dict),
                        **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._out_control_socket.send(serialize_message(ack_message))
        self._logger_thread.log("send", before=False, **log_info)

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
            if isinstance(specific_message_obj, PongMessage):
                self._forward_message_to_control_socket(message=message_obj, frame=message[1])
            elif isinstance(specific_message_obj, ReadyToReceiveMessage):
                self._forward_message_to_control_socket(message=message_obj, frame=message[1])
            elif isinstance(specific_message_obj, AckMessage):
                self._forward_message_to_control_socket(message=message_obj, frame=message[1])
            elif isinstance(specific_message_obj, PayloadMessage):
                peer = Peer(connection_info=specific_message_obj.connection_info)
                self._forward_to_pipe_socket(message=message_obj, payload=message[2:], peer=peer)
            else:
                self._logger.error("Unknown message type", message=specific_message_obj.dict(), **log_info)
        except Exception as e:
            self._logger.exception(
                "Could not deserialize message",
                message=message[1].bytes,
                **log_info
            )

    def _forward_to_pipe_socket(self, message: Message, payload: List[zmq.Frame], peer: Peer):
        log_info = dict(location="_forward_to_pipe_socket",
                        message=LazyValue(message.__root__.dict),
                        peer=LazyValue(peer.dict),
                        **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._pipe_socket_for_peers[peer].send_multipart(payload)
        self._logger_thread.log("send", before=False, **log_info)

    def _forward_message_to_control_socket(self, message: Message, frame: zmq.Frame):
        log_info = dict(location="_forward_message_to_control_socket",
                        message=LazyValue(message.__root__.dict),
                        **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._out_control_socket.send(frame)
        self._logger_thread.log("send", before=False, **log_info)

    def _set_my_connection_info(self, port: int):
        self.my_connection_info = ConnectionInfo(
            name=self._name,
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier)
        message = MyConnectionInfoMessage(connection_info=self.my_connection_info)
        log_info = dict(location="_set_my_connection_info",
                        message=LazyValue(message.dict),
                        **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._out_control_socket.send(serialize_message(message))
        self._logger_thread.log("send", before=False, **log_info)

    def _add_pipe_socket_for_peer(self, peer: Peer):
        # log = LOGGER.bind(
        #     location="_add_pipe_socket_for_peer",
        #     peer=peer.dict())

        if peer in self._pipe_socket_for_peers:
            # log.debug("Already exists")
            return

        # log.debug("Create Socket", before=True)
        pipe_socket = self._context.socket(zmq.PAIR)
        # log.debug("Create Socket", before=False)
        socket_name = get_peer_receive_socket_name(peer)

        # log = log.bind(socket_name=socket_name)
        # log.debug("Connect Socket", before=True)
        pipe_socket.connect(socket_name)
        # log.debug("Connect Socket", before=False)
        self._pipe_socket_for_peers[peer] = pipe_socket
