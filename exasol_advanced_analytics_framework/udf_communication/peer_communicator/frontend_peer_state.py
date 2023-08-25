import contextlib
from typing import Optional, Generator, List

from exasol_advanced_analytics_framework.udf_communication import messages
import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    SocketType, Socket, Frame, PollerFlag

LOGGER: FilteringBoundLogger = structlog.getLogger()


class FrontendPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer):
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._socket_factory = socket_factory
        self._peer_is_ready = False
        self._create_receive_socket()

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.connect(receive_socket_address)

    @contextlib.contextmanager
    def _create_send_socket(self) -> Generator[Socket, None, None]:
        send_socket: Socket
        with self._socket_factory.create_socket(SocketType.DEALER) as send_socket:
            send_socket.connect(
                f"tcp://{self._peer.connection_info.ipaddress.ip_address}:{self._peer.connection_info.port.port}")
            yield send_socket

    def received_peer_is_ready_to_receive(self):
        self._peer_is_ready = True

    @property
    def peer_is_ready(self) -> bool:
        return self._peer_is_ready

    @property
    def receive_socket(self) -> Socket:
        return self._receive_socket

    def send(self, payload: List[Frame]):
        send_socket: Socket
        with self._create_send_socket() as send_socket:
            message = messages.Payload(source=self._my_connection_info)
            serialized_message = serialize_message(message)
            frame = self._socket_factory.create_frame(serialized_message)
            send_socket.send_multipart([frame] + payload)
            send_socket.close(linger=100)

    def recv(self, timeout_in_milliseconds: Optional[int] = None) -> List[Frame]:
        if self._receive_socket.poll(flags=PollerFlag.POLLIN, timeout_in_ms=timeout_in_milliseconds) != 0:
            return self._receive_socket.receive_multipart()

    def stop(self):
        self._receive_socket.close(linger=0)
