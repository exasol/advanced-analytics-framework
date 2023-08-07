from typing import Callable

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state_factory import \
    BackgroundPeerStateFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder import \
    ConnectionEstablisherBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder_parameter import \
    ConnectionEstablisherBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import SenderFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, \
    SocketFactory


class BackgroundPeerStateBuilder:

    def __init__(self,
                 connection_establisher_factory: ConnectionEstablisherBuilder,
                 sender_factory: SenderFactory,
                 background_peer_state_factory: BackgroundPeerStateFactory = BackgroundPeerStateFactory()):
        self._background_peer_state_factory = background_peer_state_factory
        self._sender_factory = sender_factory
        self._connection_establisher_factory = connection_establisher_factory

    def create(
            self,
            my_connection_info: ConnectionInfo,
            out_control_socket: Socket,
            socket_factory: SocketFactory,
            peer: Peer,
            clock: Clock,
            send_socket_linger_time_in_ms: int,
            connection_establisher_builder_parameter: ConnectionEstablisherBuilderParameter,
            background_peer_state_factory: Callable = BackgroundPeerState) -> BackgroundPeerState:
        sender = self._sender_factory.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            send_socket_linger_time_in_ms=send_socket_linger_time_in_ms)
        connection_establisher = self._connection_establisher_factory.create(
            peer=peer,
            my_connection_info=my_connection_info,
            out_control_socket=out_control_socket,
            clock=clock,
            sender=sender,
            parameter=connection_establisher_builder_parameter
        )
        peer_state = self._background_peer_state_factory.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            sender=sender,
            connection_establisher=connection_establisher,
        )
        return peer_state
