from typing import List, Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message, AcknowledgeConnectionMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender, AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender, AcknowledgeRegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender, PeerIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender, RegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender, SenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender, SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    SocketType, Socket, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundPeerStateFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               socket_factory: SocketFactory,
               peer: Peer,
               forward_register_peer: bool,
               acknowledge_register_peer: bool,
               needs_register_peer_complete: bool,
               register_peer_connection: Optional[RegisterPeerConnection],
               sender: Sender,
               synchronize_connection_sender: SynchronizeConnectionSender,
               abort_timeout_sender: AbortTimeoutSender,
               peer_is_ready_sender: PeerIsReadySender,
               register_peer_sender: RegisterPeerSender,
               acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender) -> "BackgroundPeerState":
        return BackgroundPeerState(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            forward_register_peer=forward_register_peer,
            acknowledge_register_peer=acknowledge_register_peer,
            needs_register_peer_complete=needs_register_peer_complete,
            register_peer_connection=register_peer_connection,
            sender=sender,
            synchronize_connection_sender=synchronize_connection_sender,
            abort_timeout_sender=abort_timeout_sender,
            peer_is_ready_sender=peer_is_ready_sender,
            register_peer_sender=register_peer_sender,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender
        )


class BackgroundPeerState:

    @classmethod
    def create(
            cls,
            my_connection_info: ConnectionInfo,
            out_control_socket: Socket,
            socket_factory: SocketFactory,
            peer: Peer,
            register_peer_connection: Optional[RegisterPeerConnection],
            forward_register_peer: bool,
            acknowledge_register_peer: bool,
            needs_register_peer_complete: bool,
            clock: Clock,
            synchronize_timeout_in_ms: int,
            abort_timeout_in_ms: int,
            peer_is_ready_wait_time_in_ms: int,
            send_socket_linger_time_in_ms: int,
            sender_factory: SenderFactory = SenderFactory(),
            synchronize_connection_sender_factory: SynchronizeConnectionSenderFactory =
            SynchronizeConnectionSenderFactory(),
            abort_timeout_sender_factory: AbortTimeoutSenderFactory = AbortTimeoutSenderFactory(),
            peer_is_ready_sender_factory: PeerIsReadySenderFactory = PeerIsReadySenderFactory(),
            register_peer_sender_factory: RegisterPeerSenderFactory = RegisterPeerSenderFactory(),
            acknowledge_register_peer_sender_factory: AcknowledgeRegisterPeerSenderFactory =
            AcknowledgeRegisterPeerSenderFactory(),
            timer_factory: TimerFactory = TimerFactory(),
            background_peer_state_factory: BackgroundPeerStateFactory = BackgroundPeerStateFactory()
    ):
        sender = sender_factory.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            send_socket_linger_time_in_ms=send_socket_linger_time_in_ms)
        synchronize_connection_sender_timer = timer_factory.create(clock=clock, timeout_in_ms=synchronize_timeout_in_ms)
        synchronize_connection_sender = synchronize_connection_sender_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=synchronize_connection_sender_timer
        )
        needs_acknowledge_register_peer = (register_peer_connection is not None
                                           and forward_register_peer)
        abort_timeout_sender_timer = timer_factory.create(clock=clock, timeout_in_ms=abort_timeout_in_ms)
        abort_timeout_sender = abort_timeout_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=abort_timeout_sender_timer,
            my_connection_info=my_connection_info,
            peer=peer,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer
        )
        needs_register_peer_complete_and_predecessor_exists = (register_peer_connection is not None
                                                               and register_peer_connection.predecssor is not None
                                                               and needs_register_peer_complete)
        peer_is_ready_sender_timer = timer_factory.create(clock=clock, timeout_in_ms=peer_is_ready_wait_time_in_ms)
        peer_is_ready_sender = peer_is_ready_sender_factory.create(
            out_control_socket=out_control_socket,
            timer=peer_is_ready_sender_timer,
            peer=peer,
            my_connection_info=my_connection_info,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer,
            needs_register_peer_complete=needs_register_peer_complete_and_predecessor_exists,
        )
        register_peer_timer = timer_factory.create(clock=clock, timeout_in_ms=synchronize_timeout_in_ms)
        register_peer_sender = register_peer_sender_factory.create(
            register_peer_connection=register_peer_connection,
            needs_to_send_for_peer=forward_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=register_peer_timer,
        )
        acknowledge_register_peer_sender_timer = timer_factory.create(clock=clock,
                                                                      timeout_in_ms=synchronize_timeout_in_ms)
        acknowledge_register_peer_sender = acknowledge_register_peer_sender_factory.create(
            register_peer_connection=register_peer_connection,
            needs_to_send_for_peer=acknowledge_register_peer,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=acknowledge_register_peer_sender_timer,
        )
        peer_state = background_peer_state_factory.create(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            forward_register_peer=forward_register_peer,
            acknowledge_register_peer=acknowledge_register_peer,
            needs_register_peer_complete=needs_register_peer_complete,
            register_peer_connection=register_peer_connection,
            sender=sender,
            synchronize_connection_sender=synchronize_connection_sender,
            abort_timeout_sender=abort_timeout_sender,
            peer_is_ready_sender=peer_is_ready_sender,
            register_peer_sender=register_peer_sender,
            acknowledge_register_peer_sender=acknowledge_register_peer_sender
        )
        return peer_state

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 forward_register_peer: bool,
                 acknowledge_register_peer: bool,
                 needs_register_peer_complete: bool,
                 register_peer_connection: Optional[RegisterPeerConnection],
                 sender: Sender,
                 synchronize_connection_sender: SynchronizeConnectionSender,
                 abort_timeout_sender: AbortTimeoutSender,
                 peer_is_ready_sender: PeerIsReadySender,
                 register_peer_sender: RegisterPeerSender,
                 acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender):
        self._acknowledge_register_peer_sender = acknowledge_register_peer_sender
        self._register_peer_sender = register_peer_sender
        self._register_peer_connection = register_peer_connection
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._socket_factory = socket_factory
        self._create_receive_socket()
        self._sender = sender
        self._synchronize_connection_sender = synchronize_connection_sender
        self._abort_timeout_sender = abort_timeout_sender
        self._peer_is_ready_sender = peer_is_ready_sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
            forward_register_peer=forward_register_peer,
            acknowledge_register_peer=acknowledge_register_peer,
            needs_register_peer_complete=needs_register_peer_complete
        )
        self._logger.debug("__init__")
        self._register_peer_sender.try_send(force=True)
        self._acknowledge_register_peer_sender.try_send(force=True)
        self._synchronize_connection_sender.try_send(force=True)

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    def resend_if_necessary(self):
        self._logger.debug("resend_if_necessary")
        senders = [
            self._register_peer_sender,
            self._synchronize_connection_sender,
            self._abort_timeout_sender,
            self._peer_is_ready_sender,
            self._acknowledge_register_peer_sender
        ]
        for sender in senders:
            try_send = getattr(sender, "try_send")
            try_send()

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._peer_is_ready_sender.received_synchronize_connection()
        self._peer_is_ready_sender.reset_timer()
        self._abort_timeout_sender.received_synchronize_connection()
        self._sender.send(Message(__root__=AcknowledgeConnectionMessage(source=self._my_connection_info)))

    def received_acknowledge_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._abort_timeout_sender.received_acknowledge_connection()
        self._peer_is_ready_sender.received_acknowledge_connection()
        self._synchronize_connection_sender.stop()

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._register_peer_connection.complete(self._peer)
        self._peer_is_ready_sender.received_acknowledge_register_peer()
        self._peer_is_ready_sender.reset_timer()
        self._abort_timeout_sender.received_acknowledge_register_peer()
        self._register_peer_sender.stop()

    def received_register_peer_complete(self):
        self._logger.debug("received_register_peer_complete")
        self._peer_is_ready_sender.received_register_peer_complete()
        self._acknowledge_register_peer_sender.stop()

    def forward_payload(self, frames: List[Frame]):
        self._receive_socket.send_multipart(frames)

    def close(self):
        self._receive_socket.close(linger=0)
