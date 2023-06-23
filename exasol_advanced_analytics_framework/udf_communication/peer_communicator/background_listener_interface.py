import threading
import traceback
from typing import Optional, Iterator

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    MyConnectionInfoMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_thread import \
    BackgroundListenerThread
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    SocketType, Socket, PollerFlag

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundListenerInterface:

    def __init__(self,
                 name: str,
                 socket_factory: SocketFactory,
                 listen_ip: IPAddress,
                 group_identifier: str):
        self._name = name
        self._logger = LOGGER.bind(
            module_name=__name__,
            clazz=self.__class__.__name__,
            name=self._name,
            group_identifier=group_identifier
        )
        out_control_socket_address = self._create_out_control_socket(socket_factory)
        in_control_socket_address = self._create_in_control_socket(socket_factory)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._background_listener_run = BackgroundListenerThread(
            name=self._name,
            socket_factory=socket_factory,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            out_control_socket_address=out_control_socket_address,
            in_control_socket_address=in_control_socket_address,
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
            self._logger.exception("Exception",
                                   location="_set_my_connection_info",
                                   raw_message=message,
                                   exception=traceback.format_exc())

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def register_peer(self, peer: Peer):
        register_message = RegisterPeerMessage(peer=peer)
        self._in_control_socket.send(serialize_message(register_message))

    def receive_messages(self, timeout_in_milliseconds: Optional[int] = 0) -> Iterator[Message]:
        while PollerFlag.POLLIN in self._out_control_socket.poll(
                flags=PollerFlag.POLLIN,
                timeout_in_ms=timeout_in_milliseconds):
            message = None
            try:
                message = self._out_control_socket.receive()
                message_obj: Message = deserialize_message(message, Message)
                specific_message_obj = message_obj.__root__
                timeout_in_milliseconds = 0
                yield specific_message_obj
            except Exception as e:
                self._logger.exception("Exception",
                                       location="receive_messages",
                                       raw_message=message,
                                       exception=traceback.format_exc())

    def close(self):
        logger = self._logger.bind(location="close")
        self._logger.info("start")
        self._send_stop()
        self._thread.join()
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        self._logger.info("end")

    def _send_stop(self):
        stop_message = StopMessage()
        self._in_control_socket.send(serialize_message(stop_message))
