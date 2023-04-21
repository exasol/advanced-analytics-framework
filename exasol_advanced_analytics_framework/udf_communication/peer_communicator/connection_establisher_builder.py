from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder_parameter import \
    ConnectionEstablisherBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_factory import \
    ConnectionEstablisherFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket


def _needs_acknowledge_register_peer(parameter: ConnectionEstablisherBuilderParameter):
    needs_acknowledge_register_peer = \
        (parameter.register_peer_connection is not None
         and parameter.behavior_config.forward_register_peer)
    return needs_acknowledge_register_peer


class ConnectionEstablisherBuilder:

    def __init__(self,
                 abort_timeout_sender_factory: AbortTimeoutSenderFactory,
                 acknowledge_register_peer_sender_factory: AcknowledgeRegisterPeerSenderFactory,
                 peer_is_ready_sender_factory: PeerIsReadySenderFactory,
                 register_peer_sender_factory: RegisterPeerSenderFactory,
                 synchronize_connection_sender_factory: SynchronizeConnectionSenderFactory,
                 timer_factory: TimerFactory,
                 connection_establisher_factory: ConnectionEstablisherFactory =
                 ConnectionEstablisherFactory()):
        self._connection_establisher_factory = connection_establisher_factory
        self._timer_factory = timer_factory
        self._synchronize_connection_sender_factory = synchronize_connection_sender_factory
        self._register_peer_sender_factory = register_peer_sender_factory
        self._peer_is_ready_sender_factory = peer_is_ready_sender_factory
        self._acknowledge_register_peer_sender_factory = acknowledge_register_peer_sender_factory
        self._abort_timeout_sender_factory = abort_timeout_sender_factory

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               out_control_socket: Socket,
               clock: Clock,
               sender: Sender,
               parameter: ConnectionEstablisherBuilderParameter) -> ConnectionEstablisher:
        synchronize_connection_sender = self._create_synchronize_connection_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            clock=clock,
            parameter=parameter)
        abort_timeout_sender = self._create_abort_timeout_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            out_control_socket=out_control_socket,
            clock=clock,
            parameter=parameter)
        peer_is_ready_sender = self._create_peer_is_ready_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            clock=clock,
            out_control_socket=out_control_socket,
            parameter=parameter
        )
        register_peer_sender = self.create_register_peer_sender(
            peer=peer,
            my_connection_info=my_connection_info,
            clock=clock,
            parameter=parameter)
        acknowledge_register_peer_sender = \
            self.create_acknowledge_register_peer_sender(
                my_connection_info=my_connection_info,
                peer=peer,
                clock=clock,
                parameter=parameter)
        return self._connection_establisher_factory.create(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            register_peer_connection=parameter.register_peer_connection,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender,
            abort_timeout_sender=abort_timeout_sender,
            peer_is_ready_sender=peer_is_ready_sender,
            register_peer_sender=register_peer_sender,
            synchronize_connection_sender=synchronize_connection_sender
        )

    def create_acknowledge_register_peer_sender(self,
                                                my_connection_info: ConnectionInfo, peer: Peer,
                                                clock: Clock, parameter: ConnectionEstablisherBuilderParameter):
        acknowledge_register_peer_sender_timer = self._timer_factory.create(
            clock=clock,
            timeout_in_ms=parameter.timeout_config.acknowledge_register_peer_retry_timeout_in_ms)
        acknowledge_register_peer_sender = self._acknowledge_register_peer_sender_factory.create(
            register_peer_connection=parameter.register_peer_connection,
            needs_to_send_for_peer=parameter.behavior_config.acknowledge_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=acknowledge_register_peer_sender_timer,
        )
        return acknowledge_register_peer_sender

    def create_register_peer_sender(self,
                                    my_connection_info: ConnectionInfo, peer: Peer,
                                    clock: Clock, parameter: ConnectionEstablisherBuilderParameter):
        register_peer_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.register_peer_retry_timeout_in_ms)
        register_peer_sender = self._register_peer_sender_factory.create(
            register_peer_connection=parameter.register_peer_connection,
            needs_to_send_for_peer=parameter.behavior_config.forward_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=register_peer_sender_timer,
        )
        return register_peer_sender

    def _create_peer_is_ready_sender(self,
                                     my_connection_info: ConnectionInfo, peer: Peer,
                                     clock: Clock, out_control_socket: Socket,
                                     parameter: ConnectionEstablisherBuilderParameter):
        needs_register_peer_complete_and_predecessor_exists = \
            (parameter.register_peer_connection is not None
             and parameter.register_peer_connection.predecssor is not None
             and parameter.behavior_config.needs_register_peer_complete)
        peer_is_ready_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.peer_is_ready_wait_time_in_ms)
        peer_is_ready_sender = self._peer_is_ready_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=peer_is_ready_sender_timer,
            peer=peer,
            my_connection_info=my_connection_info,
            needs_acknowledge_register_peer=_needs_acknowledge_register_peer(parameter),
            needs_register_peer_complete=needs_register_peer_complete_and_predecessor_exists,
        )
        return peer_is_ready_sender

    def _create_abort_timeout_sender(self,
                                     my_connection_info: ConnectionInfo, peer: Peer,
                                     out_control_socket: Socket, clock: Clock,
                                     parameter: ConnectionEstablisherBuilderParameter):
        abort_timeout_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.abort_timeout_in_ms)
        abort_timeout_sender = self._abort_timeout_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=abort_timeout_sender_timer,
            my_connection_info=my_connection_info,
            peer=peer,
            needs_acknowledge_register_peer=_needs_acknowledge_register_peer(parameter)
        )
        return abort_timeout_sender

    def _create_synchronize_connection_sender(self,
                                              my_connection_info: ConnectionInfo, peer: Peer,
                                              sender: Sender, clock: Clock,
                                              parameter: ConnectionEstablisherBuilderParameter):
        synchronize_connection_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=parameter.timeout_config.synchronize_retry_timeout_in_ms)
        synchronize_connection_sender = self._synchronize_connection_sender_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=synchronize_connection_sender_timer
        )
        return synchronize_connection_sender
