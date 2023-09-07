from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state_factory import \
    BackgroundPeerStateFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder import \
    ConnectionEstablisherBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config \
    import ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler_builder import \
    PayloadHandlerBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_timeout_config \
    import PayloadMessageSenderTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_builder import \
    RegisterPeerForwarderBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_builder_parameter \
    import RegisterPeerForwarderBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import SenderFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, \
    SocketFactory


class BackgroundPeerStateBuilder:

    def __init__(self,
                 connection_establisher_builder: ConnectionEstablisherBuilder,
                 register_peer_forwarder_builder: RegisterPeerForwarderBuilder,
                 payload_handler_builder: PayloadHandlerBuilder,
                 sender_factory: SenderFactory,
                 background_peer_state_factory: BackgroundPeerStateFactory = BackgroundPeerStateFactory()):
        self._payload_handler_builder = payload_handler_builder
        self._connection_establisher_builder = connection_establisher_builder
        self._register_peer_forwarder_builder = register_peer_forwarder_builder
        self._background_peer_state_factory = background_peer_state_factory
        self._sender_factory = sender_factory

    def create(
            self,
            my_connection_info: ConnectionInfo,
            out_control_socket: Socket,
            socket_factory: SocketFactory,
            peer: Peer,
            clock: Clock,
            send_socket_linger_time_in_ms: int,
            register_peer_forwarder_builder_parameter: RegisterPeerForwarderBuilderParameter,
            connection_establisher_timeout_config: ConnectionEstablisherTimeoutConfig,
            payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig
    ) -> BackgroundPeerState:
        sender = self._sender_factory.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            send_socket_linger_time_in_ms=send_socket_linger_time_in_ms)
        connection_establisher = self._connection_establisher_builder.create(
            peer=peer,
            my_connection_info=my_connection_info,
            out_control_socket=out_control_socket,
            clock=clock,
            sender=sender,
            timeout_config=connection_establisher_timeout_config
        )
        register_peer_forwarder = self._register_peer_forwarder_builder.create(
            peer=peer,
            my_connection_info=my_connection_info,
            out_control_socket=out_control_socket,
            clock=clock,
            sender=sender,
            parameter=register_peer_forwarder_builder_parameter,
        )
        payload_handler = self._payload_handler_builder.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            out_control_socket=out_control_socket,
            payload_message_sender_timeout_config=payload_message_sender_timeout_config,
            sender=sender,
            clock=clock
        )
        peer_state = self._background_peer_state_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            connection_establisher=connection_establisher,
            register_peer_forwarder=register_peer_forwarder,
            payload_handler=payload_handler
        )
        return peer_state
