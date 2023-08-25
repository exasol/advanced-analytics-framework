import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import AcknowledgeConnection, Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_is_ready_sender import \
    PeerIsReadySender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSender

LOGGER: FilteringBoundLogger = structlog.get_logger()


class ConnectionEstablisher:
    def __init__(self,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 sender: Sender,
                 register_peer_connection: RegisterPeerConnection,
                 abort_timeout_sender: AbortTimeoutSender,
                 acknowledge_register_peer_sender: AcknowledgeRegisterPeerSender,
                 peer_is_ready_sender: PeerIsReadySender,
                 register_peer_sender: RegisterPeerSender,
                 synchronize_connection_sender: SynchronizeConnectionSender):
        self._synchronize_connection_sender = synchronize_connection_sender
        self._peer_is_ready_sender = peer_is_ready_sender
        self._register_peer_sender = register_peer_sender
        self._acknowledge_register_peer_sender = acknowledge_register_peer_sender
        self._abort_timeout_sender = abort_timeout_sender
        self._register_peer_connection = register_peer_connection
        self._my_connection_info = my_connection_info
        self._peer = peer
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._send_initial_messages()

    def _send_initial_messages(self):
        self._register_peer_sender.try_send(force=True)
        self._acknowledge_register_peer_sender.try_send(force=True)
        self._synchronize_connection_sender.try_send(force=True)

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._peer_is_ready_sender.received_synchronize_connection()
        self._peer_is_ready_sender.reset_timer()
        self._abort_timeout_sender.received_synchronize_connection()
        self._sender.send(Message(__root__=AcknowledgeConnection(source=self._my_connection_info)))

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

    def try_send(self):
        self._register_peer_sender.try_send()
        self._synchronize_connection_sender.try_send()
        self._abort_timeout_sender.try_send()
        self._peer_is_ready_sender.try_send()
        self._acknowledge_register_peer_sender.try_send()

    def is_ready_to_stop(self):
        peer_is_ready_sender = self._peer_is_ready_sender.is_ready_to_stop()
        register_peer_sender = self._register_peer_sender.is_ready_to_stop()
        self._logger.debug("is_ready_to_stop",
                           peer_is_ready_sender=peer_is_ready_sender,
                           register_peer_sender=register_peer_sender,
                           )
        return (
                peer_is_ready_sender
                and register_peer_sender
        )
