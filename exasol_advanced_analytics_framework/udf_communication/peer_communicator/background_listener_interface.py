import threading
from dataclasses import asdict
from typing import Optional, Iterator, List, Tuple

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import CloseMessage, PrepareToCloseMessage, \
    IsReadyToCloseMessage
from exasol_advanced_analytics_framework.udf_communication.messages import Message, RegisterPeerMessage, \
    MyConnectionInfoMessage, PayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_thread import \
    BackgroundListenerThread
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator_config import \
    PeerCommunicatorConfig
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory \
    import SocketFactory, SocketType, Socket, PollerFlag, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundListenerInterface:

    def __init__(self,
                 name: str,
                 number_of_peers: int,
                 socket_factory: SocketFactory,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 config: PeerCommunicatorConfig,
                 clock: Clock,
                 trace_logging: bool):
        self._config = config
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=group_identifier,
            config=asdict(config),
        )
        self._socket_factory = socket_factory
        out_control_socket_address = self._create_out_control_socket(socket_factory)
        in_control_socket_address = self._create_in_control_socket(socket_factory)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._is_ready_to_close = False
        self._background_listener_run = BackgroundListenerThread(
            name=self._name,
            number_of_peers=number_of_peers,
            socket_factory=socket_factory,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            out_control_socket_address=out_control_socket_address,
            in_control_socket_address=in_control_socket_address,
            clock=clock,
            config=config,
            trace_logging=trace_logging,
        )
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.daemon = True
        self._thread.start()
        self._set_my_connection_info()

    def _create_in_control_socket(self, socket_factory: SocketFactory) -> str:
        self._in_control_socket: Socket = socket_factory.create_socket(SocketType.PAIR)
        in_control_socket_address = f"inproc://BackgroundListener_in_control_socket{id(self)}"
        self._in_control_socket.bind(in_control_socket_address)
        return in_control_socket_address

    def _create_out_control_socket(self, socket_factory: SocketFactory) -> str:
        self._out_control_socket: Socket = socket_factory.create_socket(SocketType.PAIR)
        out_control_socket_address = f"inproc://BackgroundListener_out_control_socket{id(self)}"
        self._out_control_socket.bind(out_control_socket_address)
        return out_control_socket_address

    def _set_my_connection_info(self):
        message = None
        try:
            message = self._out_control_socket.receive()
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            assert isinstance(specific_message_obj, MyConnectionInfoMessage)
            self._my_connection_info = specific_message_obj.my_connection_info
        except Exception as e:
            self._logger.exception("Exception", raw_message=message)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def send_payload(self, message: PayloadMessage, payload: List[Frame]):
        message = serialize_message(message)
        frame = self._socket_factory.create_frame(message)
        self._in_control_socket.send_multipart([frame] + payload)

    def register_peer(self, peer: Peer):
        register_message = RegisterPeerMessage(peer=peer)
        self._in_control_socket.send(serialize_message(register_message))

    def receive_messages(self, timeout_in_milliseconds: Optional[int] = 0) -> Iterator[Tuple[Message, List[Frame]]]:
        while PollerFlag.POLLIN in self._out_control_socket.poll(
                flags=PollerFlag.POLLIN,
                timeout_in_ms=timeout_in_milliseconds):
            message = None
            try:
                timeout_in_milliseconds = 0
                frames = self._out_control_socket.receive_multipart()
                message_obj: Message = deserialize_message(frames[0].to_bytes(), Message)
                yield from self._handle_message(message_obj, frames[1:])
            except Exception as e:
                self._logger.exception("Exception", raw_message=message)

    def _handle_message(self, message_obj: Message, frames: List[Frame]) -> Tuple[Message, List[Frame]]:
        specific_message_obj = message_obj.__root__
        if isinstance(specific_message_obj, IsReadyToCloseMessage):
            self._is_ready_to_close = True
        else:
            yield message_obj, frames

    def is_ready_to_close(self):
        return self._is_ready_to_close

    def close(self):
        self._logger.info("start")
        self._send_close()
        self._thread.join()
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        self._logger.info("end")

    def _send_close(self):
        close_message = CloseMessage()
        self._in_control_socket.send(serialize_message(close_message))

    def prepare_to_close(self):
        self._logger.info("start")
        self._send_prepare_to_close()
        self._logger.info("end")

    def _send_prepare_to_close(self):
        prepare_to_close_message = PrepareToCloseMessage()
        self._in_control_socket.send(serialize_message(prepare_to_close_message))
