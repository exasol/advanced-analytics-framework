import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.send_socket_factory import \
    SendSocketFactory
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketFactory

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class Sender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 send_socket_linger_time_in_ms: int):
        self._send_socket_linger_time_in_ms = send_socket_linger_time_in_ms
        self._send_socket_factory = SendSocketFactory(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer
        )

    def send(self, message: Message):
        with self._send_socket_factory.create_send_socket() as send_socket:
            serialized_message = serialize_message(message.__root__)
            send_socket.send(serialized_message)
            send_socket.close(self._send_socket_linger_time_in_ms)


class SenderFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               socket_factory: SocketFactory,
               peer: Peer,
               send_socket_linger_time_in_ms: int) -> Sender:
        sender = Sender(my_connection_info=my_connection_info,
                        socket_factory=socket_factory,
                        peer=peer,
                        send_socket_linger_time_in_ms=send_socket_linger_time_in_ms)
        return sender
