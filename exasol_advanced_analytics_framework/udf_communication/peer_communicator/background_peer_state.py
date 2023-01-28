from typing import List, Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message, AcknowledgeConnectionMessage, \
    RegisterPeerMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.get_peer_receive_socket_name import \
    get_peer_receive_socket_name
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket, SocketFactory, \
    SocketType, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


class BackgroundPeerState:

    @classmethod
    def create(
            cls,
            my_connection_info: ConnectionInfo,
            out_control_socket: Socket,
            socket_factory: SocketFactory,
            peer: Peer,
            register_peer_connection: Optional[RegisterPeerConnection],
            clock: Clock,
            synchronize_timeout_in_ms: int,
            abort_timeout_in_ms: int,
            peer_is_ready_wait_time_in_ms: int,
            send_socket_linger_time_in_ms: int
    ):
        sender = Sender(my_connection_info=my_connection_info,
                        socket_factory=socket_factory,
                        peer=peer,
                        send_socket_linger_time_in_ms=send_socket_linger_time_in_ms)
        synchronize_connection_sender = SynchronizeConnectionSender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=Timer(clock=clock, timeout_in_ms=synchronize_timeout_in_ms)
        )
        needs_acknowledge_register_peer = register_peer_connection is not None
        abort_timeout_sender = AbortTimeoutSender(
            out_control_socket=out_control_socket,
            timer=Timer(clock=clock, timeout_in_ms=abort_timeout_in_ms),
            my_connection_info=my_connection_info,
            peer=peer,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer
        )
        peer_is_ready_sender = PeerIsReadySender(
            out_control_socket=out_control_socket,
            timer=Timer(clock=clock, timeout_in_ms=peer_is_ready_wait_time_in_ms),
            peer=peer,
            my_connection_info=my_connection_info,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer
        )
        register_peer_sender = RegisterPeerSender(
            register_peer_connection=register_peer_connection,
            my_connection_info=my_connection_info,
            peer=peer,
            timer=Timer(clock=clock, timeout_in_ms=synchronize_timeout_in_ms),
        )
        peer_state = cls(
            my_connection_info=my_connection_info,
            socket_factory=socket_factory,
            peer=peer,
            register_peer_connection=register_peer_connection,
            sender=sender,
            synchronize_connection_sender=synchronize_connection_sender,
            abort_timeout_sender=abort_timeout_sender,
            peer_is_ready_sender=peer_is_ready_sender,
            register_peer_sender=register_peer_sender
        )
        return peer_state

    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 socket_factory: SocketFactory,
                 peer: Peer,
                 register_peer_connection: Optional[RegisterPeerConnection],
                 sender: Sender,
                 synchronize_connection_sender: SynchronizeConnectionSender,
                 abort_timeout_sender: AbortTimeoutSender,
                 peer_is_ready_sender: PeerIsReadySender,
                 register_peer_sender: RegisterPeerSender):
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
            forwarded=self._register_peer_connection is not None
        )
        self._logger.debug("__init__")
        self._register_peer_sender.try_send(force=True)
        self._synchronize_connection_sender.try_send(force=True)

    def _create_receive_socket(self):
        self._receive_socket = self._socket_factory.create_socket(SocketType.PAIR)
        receive_socket_address = get_peer_receive_socket_name(self._peer)
        self._receive_socket.bind(receive_socket_address)

    def resend_if_necessary(self):
        self._logger.debug("resend_if_necessary")
        self._register_peer_sender.try_send()
        self._synchronize_connection_sender.try_send()
        self._abort_timeout_sender.try_send()
        self._peer_is_ready_sender.try_send()

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._peer_is_ready_sender.received_synchronize_connection()
        self._peer_is_ready_sender.reset_timer()
        self._abort_timeout_sender.received_synchronize_connection()
        self._sender.send(Message(__root__=AcknowledgeConnectionMessage(source=self._my_connection_info)))

    def received_acknowledge_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._abort_timeout_sender.received_acknowledge_connection()
        self._synchronize_connection_sender.stop()
        self._peer_is_ready_sender.received_acknowledge_connection()

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._peer_is_ready_sender.received_acknowledge_register_peer()
        self._abort_timeout_sender.received_acknowledge_register_peer()
        self._register_peer_sender.stop()

    def forward_payload(self, frames: List[Frame]):
        self._receive_socket.send_multipart(frames)

    def close(self):
        self._receive_socket.close(linger=0)
