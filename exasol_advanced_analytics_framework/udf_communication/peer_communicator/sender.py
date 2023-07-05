from typing import Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    Socket, SocketType

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class Sender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 send_socket_linger_time_in_ms: int):
        self._send_socket_linger_time_in_ms = send_socket_linger_time_in_ms
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._socket_factory = socket_factory

        self._logger = LOGGER.bind(
            peer=self._peer,
            my_connection_info=self._my_connection_info,
        )

    def create_send_socket(self) -> Socket:
        send_socket: Optional[Socket] = None
        try:
            send_socket = self._socket_factory.create_socket(SocketType.DEALER)
            send_socket.connect(
                "tcp://{ip}:{port}".format(
                    ip=self._peer.connection_info.ipaddress.ip_address,
                    port=self._peer.connection_info.port.port
                ))
            return send_socket
        except Exception:
            send_socket.close()

    def send(self, message: Message):
        with self.create_send_socket() as send_socket:
            serialized_message = serialize_message(message.__root__)
            send_socket.send(serialized_message)
            send_socket.close(self._send_socket_linger_time_in_ms)
