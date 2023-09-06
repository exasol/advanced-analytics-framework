from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler import PayloadHandler
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler_factory import \
    PayloadHandlerFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_timeout_config \
    import PayloadMessageSenderTimeoutConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_receiver_factory import \
    PayloadReceiverFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender_factory import \
    PayloadSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket, \
    SocketFactory


class PayloadHandlerBuilder:
    def __init__(self,
                 payload_sender_factory: PayloadSenderFactory,
                 payload_receiver_factory: PayloadReceiverFactory,
                 payload_handler_factory: PayloadHandlerFactory = PayloadHandlerFactory()):
        self._payload_handler_factory = payload_handler_factory
        self._payload_receiver_factory = payload_receiver_factory
        self._payload_sender_factory = payload_sender_factory

    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               out_control_socket: Socket,
               socket_factory: SocketFactory,
               sender: Sender,
               clock: Clock,
               payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig) -> PayloadHandler:
        payload_sender = self._payload_sender_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            out_control_socket=out_control_socket,
            clock=clock,
            payload_message_sender_timeout_config=payload_message_sender_timeout_config
        )
        payload_receiver = self._payload_receiver_factory.create(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            socket_factory=socket_factory,
        )
        payload_handler = self._payload_handler_factory.create(
            payload_sender=payload_sender,
            payload_receiver=payload_receiver,
        )
        return payload_handler
