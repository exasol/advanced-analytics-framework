from typing import Optional, List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_interface import \
    BackgroundListenerInterface
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    SocketType, Socket, Frame, PollerFlag

LOGGER: FilteringBoundLogger = structlog.getLogger()


class FrontendPeerState:

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 background_listener: BackgroundListenerInterface,
                 peer: Peer):
        self._background_listener = background_listener
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._socket_factory = socket_factory
        self._connection_is_ready = False
        self._peer_register_forwarder_is_ready = False
        self._sequence_number = 0
        self._create_receive_socket()
        self._logger = LOGGER.bind(peer=peer.dict(), my_connection_info=my_connection_info.dict())

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.connect(receive_socket_address)

    def received_connection_is_ready(self):
        self._connection_is_ready = True

    def received_peer_register_forwarder_is_ready(self):
        self._peer_register_forwarder_is_ready = True

    @property
    def peer_is_ready(self) -> bool:
        return self._connection_is_ready and self._peer_register_forwarder_is_ready

    @property
    def receive_socket(self) -> Socket:
        return self._receive_socket

    def _next_sequence_number(self):
        result = self._sequence_number
        self._sequence_number += 1
        return result

    def send(self, payload: List[Frame]):
        message = messages.Payload(source=Peer(connection_info=self._my_connection_info),
                                   destination=self._peer,
                                   sequence_number=self._next_sequence_number())
        self._logger.debug("send", message=message.dict())
        self._background_listener.send_payload(message=message, payload=payload)
        return message.sequence_number

    def recv(self, timeout_in_milliseconds: Optional[int] = None) -> List[Frame]:
        if self._receive_socket.poll(flags=PollerFlag.POLLIN, timeout_in_ms=timeout_in_milliseconds) != 0:
            return self._receive_socket.receive_multipart()

    def stop(self):
        self._receive_socket.close(linger=0)
