from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher import \
    ConnectionEstablisher
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_factory import \
    ConnectionEstablisherFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_timeout_config import \
    ConnectionEstablisherTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket


class ConnectionEstablisherBuilder:

    def __init__(self,
                 timer_factory: TimerFactory,
                 abort_timeout_sender_factory: AbortTimeoutSenderFactory = AbortTimeoutSenderFactory(),
                 connection_is_ready_sender_factory: ConnectionIsReadySenderFactory = ConnectionIsReadySenderFactory(),
                 synchronize_connection_sender_factory: SynchronizeConnectionSenderFactory =
                 SynchronizeConnectionSenderFactory(),
                 connection_establisher_factory: ConnectionEstablisherFactory =
                 ConnectionEstablisherFactory()):
        self._connection_establisher_factory = connection_establisher_factory
        self._timer_factory = timer_factory
        self._synchronize_connection_sender_factory = synchronize_connection_sender_factory
        self._connection_is_ready_sender_factory = connection_is_ready_sender_factory
        self._abort_timeout_sender_factory = abort_timeout_sender_factory

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               out_control_socket: Socket,
               clock: Clock,
               sender: Sender,
               timeout_config: ConnectionEstablisherTimeoutConfig) -> ConnectionEstablisher:
        synchronize_connection_sender = self._create_synchronize_connection_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            clock=clock,
            timeout_config=timeout_config)
        abort_timeout_sender = self._create_abort_timeout_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            out_control_socket=out_control_socket,
            clock=clock,
            timeout_config=timeout_config)
        connection_is_ready_sender = self._create_connection_is_ready_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            clock=clock,
            out_control_socket=out_control_socket,
            timeout_config=timeout_config
        )
        return self._connection_establisher_factory.create(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            abort_timeout_sender=abort_timeout_sender,
            connection_is_ready_sender=connection_is_ready_sender,
            synchronize_connection_sender=synchronize_connection_sender
        )

    def _create_connection_is_ready_sender(self,
                                           my_connection_info: ConnectionInfo, peer: Peer,
                                           clock: Clock, out_control_socket: Socket,
                                           timeout_config: ConnectionEstablisherTimeoutConfig):
        connection_is_ready_sender = self._connection_is_ready_sender_factory.create(
            out_control_socket=out_control_socket,
            peer=peer,
            my_connection_info=my_connection_info,
        )
        return connection_is_ready_sender

    def _create_abort_timeout_sender(self,
                                     my_connection_info: ConnectionInfo, peer: Peer,
                                     out_control_socket: Socket, clock: Clock,
                                     timeout_config: ConnectionEstablisherTimeoutConfig):
        abort_timeout_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=timeout_config.abort_timeout_in_ms)
        abort_timeout_sender = self._abort_timeout_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=abort_timeout_sender_timer,
            my_connection_info=my_connection_info,
            peer=peer,
            reason="Timeout occurred during establishing connection."
        )
        return abort_timeout_sender

    def _create_synchronize_connection_sender(self,
                                              my_connection_info: ConnectionInfo, peer: Peer,
                                              sender: Sender, clock: Clock,
                                              timeout_config: ConnectionEstablisherTimeoutConfig):
        synchronize_connection_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=timeout_config.synchronize_retry_timeout_in_ms)
        synchronize_connection_sender = self._synchronize_connection_sender_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=synchronize_connection_sender_timer
        )
        return synchronize_connection_sender
