import dataclasses
import enum
from typing import Dict, List, Optional

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.messages import Message, RegisterPeerMessage, \
    PayloadMessage, MyConnectionInfoMessage, SynchronizeConnectionMessage, AcknowledgeConnectionMessage, \
    AcknowledgeRegisterPeerMessage, RegisterPeerCompleteMessage, IsReadyToCloseMessage, CloseMessage, \
    PrepareToCloseMessage, AcknowledgePayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.abort_timeout_sender import \
    AbortTimeoutSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.acknowledge_register_peer_sender import \
    AcknowledgeRegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state import \
    BackgroundPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_peer_state_builder import \
    BackgroundPeerStateBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_behavior_config \
    import RegisterPeerForwarderBehaviorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_builder import \
    ConnectionEstablisherBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_is_ready_sender import \
    ConnectionIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler_builder import \
    PayloadHandlerBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_message_sender_factory import \
    PayloadMessageSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_receiver_factory import \
    PayloadReceiverFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender_factory import \
    PayloadSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator_config import \
    PeerCommunicatorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_builder import \
    RegisterPeerForwarderBuilder
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_builder_parameter \
    import RegisterPeerForwarderBuilderParameter
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_forwarder_is_ready_sender \
    import RegisterPeerForwarderIsReadySenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_sender import \
    RegisterPeerSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.send_socket_factory import \
    SendSocketFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import SenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.synchronize_connection_sender import \
    SynchronizeConnectionSenderFactory
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import TimerFactory
from exasol_advanced_analytics_framework.udf_communication.serialization import deserialize_message, serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory \
    import SocketFactory, SocketType, Socket, PollerFlag, Frame

LOGGER: FilteringBoundLogger = structlog.get_logger()


def create_background_peer_state_builder() -> BackgroundPeerStateBuilder:
    timer_factory = TimerFactory()
    abort_timeout_sender_factory = AbortTimeoutSenderFactory()
    peer_is_ready_sender_factory = ConnectionIsReadySenderFactory()
    synchronize_connection_sender_factory = SynchronizeConnectionSenderFactory()
    acknowledge_register_peer_sender_factory = AcknowledgeRegisterPeerSenderFactory()
    register_peer_sender_factory = RegisterPeerSenderFactory()
    register_peer_forwarder_is_ready_sender_factory = RegisterPeerForwarderIsReadySenderFactory()
    connection_establisher_builder = ConnectionEstablisherBuilder(
        abort_timeout_sender_factory=abort_timeout_sender_factory,
        synchronize_connection_sender_factory=synchronize_connection_sender_factory,
        connection_is_ready_sender_factory=peer_is_ready_sender_factory,
        timer_factory=timer_factory,
    )
    register_peer_forwarder_builder = RegisterPeerForwarderBuilder(
        abort_timeout_sender_factory=abort_timeout_sender_factory,
        register_peer_sender_factory=register_peer_sender_factory,
        acknowledge_register_peer_sender_factory=acknowledge_register_peer_sender_factory,
        register_peer_forwarder_is_ready_sender_factory=register_peer_forwarder_is_ready_sender_factory,
        timer_factory=timer_factory,
    )
    sender_factory = SenderFactory()
    payload_message_sender_factory = PayloadMessageSenderFactory(timer_factory=timer_factory)
    payload_sender_factory = PayloadSenderFactory(payload_message_sender_factory=payload_message_sender_factory)
    payload_receiver_factory = PayloadReceiverFactory()
    payload_handler_builder = PayloadHandlerBuilder(
        payload_sender_factory=payload_sender_factory,
        payload_receiver_factory=payload_receiver_factory
    )
    background_peer_state_factory = BackgroundPeerStateBuilder(
        sender_factory=sender_factory,
        connection_establisher_builder=connection_establisher_builder,
        register_peer_forwarder_builder=register_peer_forwarder_builder,
        payload_handler_builder=payload_handler_builder,
    )
    return background_peer_state_factory


class BackgroundListenerThread:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        PREPARE_TO_CLOSED = enum.auto()
        CLOSED = enum.auto()

    def __init__(self,
                 name: str,
                 number_of_peers: int,
                 socket_factory: SocketFactory,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 out_control_socket_address: str,
                 in_control_socket_address: str,
                 clock: Clock,
                 config: PeerCommunicatorConfig,
                 trace_logging: bool,
                 background_peer_state_factory: BackgroundPeerStateBuilder = create_background_peer_state_builder()):
        self._number_of_peers = number_of_peers
        self._config = config
        self._background_peer_state_factory = background_peer_state_factory
        self._register_peer_connection: Optional[RegisterPeerConnection] = None
        self._trace_logging = trace_logging
        self._clock = clock
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=group_identifier,
            config=dataclasses.asdict(config)
        )
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._in_control_socket_address = in_control_socket_address
        self._out_control_socket_address = out_control_socket_address
        self._socket_factory = socket_factory
        self._status = BackgroundListenerThread.Status.RUNNING
        self._peer_state: Dict[Peer, BackgroundPeerState] = {}

    def run(self):
        self._create_in_control_socket()
        self._create_out_control_socket()
        port = self._create_listener_socket()
        self._set_my_connection_info(port)
        self._create_poller()
        try:
            self._run_message_loop()
        finally:
            self._close()

    def _close(self):
        self._logger.info("start")
        if self._register_peer_connection is not None:
            self._register_peer_connection.close()
        self._out_control_socket.close(linger=0)
        self._in_control_socket.close(linger=0)
        for peer_state in self._peer_state.values():
            peer_state.close()
        self._listener_socket.close(linger=0)
        self._logger.info("end")

    def _create_listener_socket(self):
        self._listener_socket: Socket = self._socket_factory.create_socket(SocketType.ROUTER)
        self._listener_socket.set_identity(self._name)
        port = self._listener_socket.bind_to_random_port(f"tcp://*")
        return port

    def _create_in_control_socket(self):
        self._in_control_socket: Socket = self._socket_factory.create_socket(SocketType.PAIR)
        self._in_control_socket.connect(self._in_control_socket_address)

    def _create_out_control_socket(self):
        self._out_control_socket: Socket = self._socket_factory.create_socket(SocketType.PAIR)
        self._out_control_socket.connect(self._out_control_socket_address)

    def _create_poller(self):
        self.poller = self._socket_factory.create_poller()
        self.poller.register(self._in_control_socket, flags=PollerFlag.POLLIN)
        self.poller.register(self._listener_socket, flags=PollerFlag.POLLIN)

    def _run_message_loop(self):
        try:
            while self._status != BackgroundListenerThread.Status.CLOSED:
                self._handle_message()
                self._try_send()
                self._check_is_ready_to_close()
        except Exception as e:
            self._logger.exception("Exception in message loop")

    def _check_is_ready_to_close(self):
        if self._status == BackgroundListenerThread.Status.PREPARE_TO_CLOSED:
            if self._is_ready_to_close():
                self._out_control_socket.send(serialize_message(IsReadyToCloseMessage()))

    def _is_ready_to_close(self):
        peers_status = [peer_state.is_ready_to_close()
                        for peer_state in self._peer_state.values()]
        is_ready_to_close = all(peers_status) and len(peers_status) == self._number_of_peers - 1
        return is_ready_to_close

    def _try_send(self):
        if self._status != BackgroundListenerThread.Status.CLOSED:
            for peer_state in self._peer_state.values():
                peer_state.try_send()

    def _handle_message(self):
        poll = self.poller.poll(timeout_in_ms=self._config.poll_timeout_in_ms)
        if self._in_control_socket in poll and PollerFlag.POLLIN in poll[self._in_control_socket]:
            message = self._in_control_socket.receive_multipart()
            self._status = self._handle_control_message(message)
        if self._listener_socket in poll and PollerFlag.POLLIN in poll[self._listener_socket]:
            message = self._listener_socket.receive_multipart()
            self._handle_listener_message(message)

    def _handle_control_message(self, message: List[Frame]) -> Status:
        try:
            message_obj: Message = deserialize_message(message[0].to_bytes(), Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, CloseMessage):
                return BackgroundListenerThread.Status.CLOSED
            elif isinstance(specific_message_obj, PrepareToCloseMessage):
                return BackgroundListenerThread.Status.PREPARE_TO_CLOSED
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                if self._is_register_peer_message_allowed_as_control_message():
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    self._logger.error("RegisterPeerMessage message not allowed",
                                       message_obj=specific_message_obj.dict())
            elif isinstance(specific_message_obj, PayloadMessage):
                self._peer_state[specific_message_obj.destination].send_payload(
                    message=specific_message_obj, frames=message)
            else:
                self._logger.error("Unknown message type", message_obj=specific_message_obj.dict())
        except Exception as e:
            self._logger.exception("Exception during handling message", message=message)
        return BackgroundListenerThread.Status.RUNNING

    def _is_register_peer_message_allowed_as_control_message(self):
        return self._config.forward_register_peer_config.is_enabled \
               and self._config.forward_register_peer_config.is_leader \
               or not self._config.forward_register_peer_config.is_enabled

    def _add_peer(self,
                  peer: Peer,
                  register_peer_forwarder_behavior_config: RegisterPeerForwarderBehaviorConfig =
                  RegisterPeerForwarderBehaviorConfig()):
        if peer.connection_info.group_identifier != self._my_connection_info.group_identifier:
            self._logger.error("Peer belongs to a different group",
                               my_connection_info=self._my_connection_info.dict(),
                               peer=peer.dict())
            raise ValueError("Peer belongs to a different group")
        if peer not in self._peer_state:
            parameter = RegisterPeerForwarderBuilderParameter(
                register_peer_connection=self._register_peer_connection,
                timeout_config=self._config.register_peer_forwarder_timeout_config,
                behavior_config=register_peer_forwarder_behavior_config)
            self._peer_state[peer] = self._background_peer_state_factory.create(
                my_connection_info=self._my_connection_info,
                peer=peer,
                out_control_socket=self._out_control_socket,
                socket_factory=self._socket_factory,
                clock=self._clock,
                send_socket_linger_time_in_ms=self._config.send_socket_linger_time_in_ms,
                connection_establisher_timeout_config=self._config.connection_establisher_timeout_config,
                register_peer_forwarder_builder_parameter=parameter,
                payload_message_sender_timeout_config=self._config.payload_message_sender_timeout_config,
            )

    def _handle_listener_message(self, message: List[Frame]):
        logger = self._logger.bind(
            sender=message[0].to_bytes()
        )
        try:
            message_obj: Message = deserialize_message(message[1].to_bytes(), Message)
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, SynchronizeConnectionMessage):
                self._handle_synchronize_connection(specific_message_obj)
            elif isinstance(specific_message_obj, AcknowledgeConnectionMessage):
                self._handle_acknowledge_connection(specific_message_obj)
            elif isinstance(specific_message_obj, RegisterPeerMessage):
                if self.is_register_peer_message_allowed_as_listener_message():
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    logger.error("RegisterPeerMessage message not allowed", message_obj=specific_message_obj.dict())
            elif isinstance(specific_message_obj, AcknowledgeRegisterPeerMessage):
                self._handle_acknowledge_register_peer_message(specific_message_obj)
            elif isinstance(specific_message_obj, RegisterPeerCompleteMessage):
                self._handle_register_peer_complete_message(specific_message_obj)
            elif isinstance(specific_message_obj, PayloadMessage):
                self._peer_state[specific_message_obj.source].received_payload(
                    specific_message_obj, frames=message[1:])
            elif isinstance(specific_message_obj, AcknowledgePayloadMessage):
                self._peer_state[specific_message_obj.source].received_acknowledge_payload(specific_message_obj)
            else:
                logger.error("Unknown message type", message_obj=specific_message_obj.dict())
        except Exception as e:
            logger.exception("Exception during handling message", message=message[1].to_bytes())

    def is_register_peer_message_allowed_as_listener_message(self):
        return not self._config.forward_register_peer_config.is_leader \
               and self._config.forward_register_peer_config.is_enabled

    def _handle_synchronize_connection(self, message: SynchronizeConnectionMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_synchronize_connection()

    def _handle_acknowledge_connection(self, message: AcknowledgeConnectionMessage):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_acknowledge_connection()

    def _set_my_connection_info(self, port: int):
        self._my_connection_info = ConnectionInfo(
            name=self._name,
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier)
        message = MyConnectionInfoMessage(my_connection_info=self._my_connection_info)
        self._out_control_socket.send(serialize_message(message))

    def _handle_register_peer_message(self, message: RegisterPeerMessage):
        if self._config.forward_register_peer_config.is_enabled:
            if self._register_peer_connection is None:
                self._create_register_peer_connection(message)
                self._add_peer(
                    message.peer,
                    register_peer_forwarder_behavior_config=RegisterPeerForwarderBehaviorConfig(
                        needs_to_send_acknowledge_register_peer=not self._config.forward_register_peer_config.is_leader
                    ))
            else:
                self._add_peer(
                    message.peer,
                    register_peer_forwarder_behavior_config=RegisterPeerForwarderBehaviorConfig(
                        needs_to_send_register_peer=True,
                        needs_to_send_acknowledge_register_peer=not self._config.forward_register_peer_config.is_leader,
                    ))
        else:
            self._add_peer(message.peer)

    def _create_register_peer_connection(self, message: RegisterPeerMessage):
        successor_send_socket_factory = SendSocketFactory(
            my_connection_info=self._my_connection_info,
            peer=message.peer,
            socket_factory=self._socket_factory
        )
        if message.source is not None:
            predecessor_send_socket_factory = SendSocketFactory(
                my_connection_info=self._my_connection_info,
                peer=message.source,
                socket_factory=self._socket_factory
            )
        else:
            predecessor_send_socket_factory = None
        self._register_peer_connection = RegisterPeerConnection(
            predecessor=message.source,
            predecessor_send_socket_factory=predecessor_send_socket_factory,
            successor=message.peer,
            successor_send_socket_factory=successor_send_socket_factory,
            my_connection_info=self._my_connection_info
        )

    def _handle_acknowledge_register_peer_message(self, message: AcknowledgeRegisterPeerMessage):
        if self._register_peer_connection.successor != message.source:
            self._logger.error("AcknowledgeRegisterPeerMessage message not from successor", message_obj=message.dict())
        peer = message.peer
        self._peer_state[peer].received_acknowledge_register_peer()

    def _handle_register_peer_complete_message(self, message: RegisterPeerCompleteMessage):
        if self._register_peer_connection.predecssor != message.source:
            self._logger.error("RegisterPeerCompleteMessage message not from predecssor", message_obj=message.dict())
        peer = message.peer
        self._peer_state[peer].received_register_peer_complete()
