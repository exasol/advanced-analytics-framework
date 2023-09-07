from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_factory import \
    PayloadMessageSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_timeout_config \
    import PayloadMessageSenderTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender import PayloadSender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket


class PayloadSenderFactory:

    def __init__(self, payload_message_sender_factory: PayloadMessageSenderFactory):
        self._payload_message_sender_factory = payload_message_sender_factory

    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               sender: Sender,
               clock: Clock,
               payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig,
               out_control_socket: Socket) -> PayloadSender:
        return PayloadSender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            clock=clock,
            payload_message_sender_timeout_config=payload_message_sender_timeout_config,
            out_control_socket=out_control_socket,
            payload_message_sender_factory=self._payload_message_sender_factory
        )
