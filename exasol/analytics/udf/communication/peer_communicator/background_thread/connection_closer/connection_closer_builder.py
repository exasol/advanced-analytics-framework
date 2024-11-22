from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.abort_timeout_sender import (
    AbortTimeoutSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.close_connection_sender import (
    CloseConnectionSender,
    CloseConnectionSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer import (
    ConnectionCloser,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_factory import (
    ConnectionCloserFactory,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_timeout_config import (
    ConnectionCloserTimeoutConfig,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_is_closed_sender import (
    ConnectionIsClosedSender,
    ConnectionIsClosedSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import TimerFactory
from exasol.analytics.udf.communication.socket_factory.abstract import Socket


class ConnectionCloserBuilder:

    def __init__(
        self,
        timer_factory: TimerFactory,
        abort_timeout_sender_factory: AbortTimeoutSenderFactory = AbortTimeoutSenderFactory(),
        connection_is_closed_sender_factory: ConnectionIsClosedSenderFactory = ConnectionIsClosedSenderFactory(),
        close_connection_sender_factory: CloseConnectionSenderFactory = CloseConnectionSenderFactory(),
        connection_closer_factory: ConnectionCloserFactory = ConnectionCloserFactory(),
    ):
        self._connection_closer_factory = connection_closer_factory
        self._timer_factory = timer_factory
        self._close_connection_sender_factory = close_connection_sender_factory
        self._connection_is_closed_sender_factory = connection_is_closed_sender_factory
        self._abort_timeout_sender_factory = abort_timeout_sender_factory

    def create(
        self,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        out_control_socket: Socket,
        clock: Clock,
        sender: Sender,
        timeout_config: ConnectionCloserTimeoutConfig,
    ) -> ConnectionCloser:
        close_connection_sender = self._create_close_connection_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            clock=clock,
            timeout_config=timeout_config,
        )
        abort_timeout_sender = self._create_abort_timeout_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            out_control_socket=out_control_socket,
            clock=clock,
            timeout_config=timeout_config,
        )
        connection_is_closed_sender = self._create_connection_is_closed_sender(
            my_connection_info=my_connection_info,
            peer=peer,
            clock=clock,
            out_control_socket=out_control_socket,
            timeout_config=timeout_config,
        )
        return self._connection_closer_factory.create(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            abort_timeout_sender=abort_timeout_sender,
            connection_is_closed_sender=connection_is_closed_sender,
            close_connection_sender=close_connection_sender,
        )

    def _create_connection_is_closed_sender(
        self,
        my_connection_info: ConnectionInfo,
        peer: Peer,
        clock: Clock,
        out_control_socket: Socket,
        timeout_config: ConnectionCloserTimeoutConfig,
    ) -> ConnectionIsClosedSender:
        timer = self._timer_factory.create(
            clock=clock,
            timeout_in_ms=timeout_config.connection_is_closed_wait_time_in_ms,
        )
        connection_is_closed_sender = self._connection_is_closed_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=timer,
            peer=peer,
            my_connection_info=my_connection_info,
        )
        return connection_is_closed_sender

    def _create_abort_timeout_sender(
        self,
        my_connection_info: ConnectionInfo,
        peer: Peer,
        out_control_socket: Socket,
        clock: Clock,
        timeout_config: ConnectionCloserTimeoutConfig,
    ):
        abort_timeout_sender_timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=timeout_config.abort_timeout_in_ms
        )
        abort_timeout_sender = self._abort_timeout_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=abort_timeout_sender_timer,
            my_connection_info=my_connection_info,
            peer=peer,
            reason="Timeout occurred during establishing connection.",
        )
        return abort_timeout_sender

    def _create_close_connection_sender(
        self,
        my_connection_info: ConnectionInfo,
        peer: Peer,
        sender: Sender,
        clock: Clock,
        timeout_config: ConnectionCloserTimeoutConfig,
    ) -> CloseConnectionSender:
        timer = self._timer_factory.create(
            clock=clock, timeout_in_ms=timeout_config.close_retry_timeout_in_ms
        )
        close_connection_sender = self._close_connection_sender_factory.create(
            my_connection_info=my_connection_info, peer=peer, sender=sender, timer=timer
        )
        return close_connection_sender
