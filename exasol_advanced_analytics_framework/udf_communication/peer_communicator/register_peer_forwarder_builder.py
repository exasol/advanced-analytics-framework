from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory, AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSenderFactory, AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder import \
    RegisterPeerForwarder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_builder_parameter import \
    RegisterPeerForwarderBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_factory import \
    RegisterPeerForwarderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_is_ready_sender import \
    RegisterPeerForwarderIsReadySenderFactory, RegisterPeerForwarderIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSenderFactory, RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket


class RegisterPeerForwarderBuilder:

    def __init__(self,
                 abort_timeout_sender_factory: AbortTimeoutSenderFactory,
                 acknowledge_register_peer_sender_factory: AcknowledgeRegisterPeerSenderFactory,
                 register_peer_forwarder_is_ready_sender_factory: RegisterPeerForwarderIsReadySenderFactory,
                 register_peer_sender_factory: RegisterPeerSenderFactory,
                 timer_factory: TimerFactory,
                 register_peer_forwarder_factory: RegisterPeerForwarderFactory =
                 RegisterPeerForwarderFactory()):
        self._register_peer_forwarder_factory = register_peer_forwarder_factory
        self._timer_factory = timer_factory
        self._register_peer_sender_factory = register_peer_sender_factory
        self._register_peer_forwarder_is_ready_sender_factory = register_peer_forwarder_is_ready_sender_factory
        self._acknowledge_register_peer_sender_factory = acknowledge_register_peer_sender_factory
        self._abort_timeout_sender_factory = abort_timeout_sender_factory

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               out_control_socket: Socket,
               clock: Clock,
               sender: Sender,
               parameter: RegisterPeerForwarderBuilderParameter
               ) -> RegisterPeerForwarder:
        abort_timeout_sender = self._create_abort_timeout_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            out_control_socket=out_control_socket,
            clock=clock,
            parameter=parameter)
        register_peer_sender = self._create_register_peer_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            clock=clock,
            parameter=parameter
        )
        acknowledge_register_peer_sender = self._create_acknowledge_register_peer_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            clock=clock,
            parameter=parameter
        )
        register_peer_forwarder_is_ready_sender = self._create_register_peer_forwarder_is_ready_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            out_control_socket=out_control_socket,
            clock=clock,
            parameter=parameter
        )
        return self._register_peer_forwarder_factory.create(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            abort_timeout_sender=abort_timeout_sender,
            register_peer_connection=parameter.register_peer_connection,
            register_peer_sender=register_peer_sender,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender,
            register_peer_forwarder_is_ready_sender=register_peer_forwarder_is_ready_sender,
        )

    def _create_acknowledge_register_peer_sender(
            self,
            my_connection_info: ConnectionInfo, peer: Peer,
            clock: Clock, parameter: RegisterPeerForwarderBuilderParameter) -> AcknowledgeRegisterPeerSender:
        acknowledge_register_peer_sender_timer = self._timer_factory.create(
            clock=clock,
            timeout_in_ms=parameter.timeout_config.acknowledge_register_peer_retry_timeout_in_ms)
        acknowledge_register_peer_sender = self._acknowledge_register_peer_sender_factory.create(
            register_peer_connection=parameter.register_peer_connection,
            needs_to_send_for_peer=parameter.behavior_config.needs_to_send_acknowledge_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=acknowledge_register_peer_sender_timer,
        )
        return acknowledge_register_peer_sender

    def _create_register_peer_sender(
            self,
            my_connection_info: ConnectionInfo, peer: Peer,
            clock: Clock, parameter: RegisterPeerForwarderBuilderParameter) -> RegisterPeerSender:
        register_peer_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.register_peer_retry_timeout_in_ms)
        register_peer_sender = self._register_peer_sender_factory.create(
            register_peer_connection=parameter.register_peer_connection,
            needs_to_send_for_peer=parameter.behavior_config.needs_to_send_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=register_peer_sender_timer,
        )
        return register_peer_sender

    def _create_register_peer_forwarder_is_ready_sender(
            self,
            my_connection_info: ConnectionInfo,
            peer: Peer,
            out_control_socket: Socket,
            clock: Clock,
            parameter: RegisterPeerForwarderBuilderParameter) -> RegisterPeerForwarderIsReadySender:
        register_peer_forwarder_is_ready_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.register_peer_forwarder_is_ready_wait_time_in_ms)
        register_peer_forwarder_is_ready_sender = self._register_peer_forwarder_is_ready_sender_factory.create(
            peer=peer, my_connection_info=my_connection_info,
            out_control_socket=out_control_socket,
            timer=register_peer_forwarder_is_ready_sender_timer,
            behavior_config=parameter.behavior_config
        )
        return register_peer_forwarder_is_ready_sender

    def _create_abort_timeout_sender(
            self,
            my_connection_info: ConnectionInfo, peer: Peer,
            out_control_socket: Socket, clock: Clock,
            parameter: RegisterPeerForwarderBuilderParameter) -> AbortTimeoutSender:
        abort_timeout_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.abort_timeout_in_ms)
        abort_timeout_sender = self._abort_timeout_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=abort_timeout_sender_timer,
            my_connection_info=my_connection_info,
            peer=peer,
            reason="Timeout occurred during sending register peer."
        )
        if not parameter.behavior_config.needs_to_send_register_peer:
            abort_timeout_sender.stop()
        return abort_timeout_sender
