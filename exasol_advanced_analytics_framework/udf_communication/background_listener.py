import threading
import traceback
from typing import Optional, Iterator

import structlog
import zmq
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.background_listener_run import BackgroundListenerRun
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.logger_thread import LoggerThread, LazyValue
from exasol_advanced_analytics_framework.udf_communication.messages import Message, StopMessage, RegisterPeerMessage, \
    MyConnectionInfoMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundListener:

    def __init__(self,
                 name: str,
                 context: zmq.Context,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 logger_thread: LoggerThread):
        self._name = name
        self._log_info = dict(module_name=__name__,
                              clazz=self.__class__.__name__,
                              name=self._name,
                              group_identifier=group_identifier)
        self._logger = LOGGER.bind(**self._log_info)
        self._logger_thread = logger_thread
        out_control_socket_address = self._create_out_control_socket(context)
        in_control_socket_address = self._create_in_control_socket(context)
        self._my_connection_info: Optional[ConnectionInfo] = None
        self._background_listener_run = BackgroundListenerRun(
            name=self._name,
            context=context,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            out_control_socket_address=out_control_socket_address,
            in_control_socket_address=in_control_socket_address,
            logger_thread=logger_thread
        )
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.daemon = True
        self._thread.start()
        self._set_my_connection_info()

    def _create_in_control_socket(self, context) -> str:
        self._in_control_socket: zmq.Socket = context.socket(zmq.PAIR)
        in_control_socket_address = f"inproc://BackgroundListener_in_control_socket{id(self)}"
        self._in_control_socket.bind(in_control_socket_address)
        return in_control_socket_address

    def _create_out_control_socket(self, context) -> str:
        self._out_control_socket: zmq.Socket = context.socket(zmq.PAIR)
        out_control_socket_address = f"inproc://BackgroundListener_out_control_socket{id(self)}"
        self._out_control_socket.bind(out_control_socket_address)
        return out_control_socket_address

    def _set_my_connection_info(self):
        log_info = dict(location="_set_my_connection_info", **self._log_info)
        message = None
        try:
            message = self._out_control_socket.recv()
            message_obj: Message = deserialize_message(message, Message)
            specific_message_obj = message_obj.__root__
            self._logger_thread.log("received", message=LazyValue(specific_message_obj.dict), **log_info)
            assert isinstance(specific_message_obj, MyConnectionInfoMessage)
            self._my_connection_info = specific_message_obj.my_connection_info
        except Exception as e:
            self._logger.exception("Exception",
                                   raw_message=message,
                                   exception=e,
                                   stacktrace=traceback.format_exc(),
                                   **log_info)

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def register_peer(self, peer: Peer):
        register_message = RegisterPeerMessage(peer=peer)
        log_info = dict(message=LazyValue(register_message.dict), location="register_peer", **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._in_control_socket.send(serialize_message(register_message))
        self._logger_thread.log("send", before=False, **log_info)

    def receive_messages(self, timeout_in_milliseconds: Optional[int] = 0) -> Iterator[Message]:
        log_info = dict(location="receive_messages", **self._log_info)
        while self._out_control_socket.poll(flags=zmq.POLLIN, timeout=timeout_in_milliseconds) != 0:
            message = None
            try:
                message = self._out_control_socket.recv()
                message_obj: Message = deserialize_message(message, Message)
                specific_message_obj = message_obj.__root__
                self._logger_thread.log("received",
                                        message=LazyValue(specific_message_obj.dict),
                                        **log_info)
                timeout_in_milliseconds = 0
                yield specific_message_obj
            except Exception as e:
                self._logger.exception("Exception",
                                       raw_message=message,
                                       exception=e,
                                       stacktrace=traceback.format_exc(),
                                       **log_info)

    def close(self):
        log_info = dict(location="close", **self._log_info)
        self._logger.info("start", **log_info)
        self._send_stop()
        self._thread.join()
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        self._logger.info("end", **log_info)

    def _send_stop(self):
        stop_message = StopMessage()
        log_info = dict(location="_send_stop", message=LazyValue(stop_message.dict), **self._log_info)
        self._logger_thread.log("send", before=True, **log_info)
        self._in_control_socket.send(serialize_message(stop_message))
        self._logger_thread.log("send", before=False, **log_info)
