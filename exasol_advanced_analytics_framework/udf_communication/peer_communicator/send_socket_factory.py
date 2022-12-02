from typing import Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketFactory, \
    Socket, SocketType

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class SendSocketFactory:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer):
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
                f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
            return send_socket
        except Exception:
            self._logger.exception("Error during connect")
            send_socket.close()
            raise
