import dataclasses
import enum
from typing import Dict, List, Optional

import structlog
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.messages import PrepareToStop
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.background_peer_state import (
    BackgroundPeerState,
)
from exasol.analytics.udf.communication.peer_communicator.background_peer_state_builder import (
    BackgroundPeerStateBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer_builder import (
    ConnectionCloserBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.connection_establisher_builder import (
    ConnectionEstablisherBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.payload_handler_builder import (
    PayloadHandlerBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender_factory import (
    PayloadMessageSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.payload_sender_factory import (
    PayloadSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_connection import (
    RegisterPeerConnection,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_behavior_config import (
    RegisterPeerForwarderBehaviorConfig,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_builder import (
    RegisterPeerForwarderBuilder,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_builder_parameter import (
    RegisterPeerForwarderBuilderParameter,
)
from exasol.analytics.udf.communication.peer_communicator.send_socket_factory import (
    SendSocketFactory,
)
from exasol.analytics.udf.communication.peer_communicator.sender import SenderFactory
from exasol.analytics.udf.communication.peer_communicator.timer import TimerFactory
from exasol.analytics.udf.communication.serialization import (
    deserialize_message,
    serialize_message,
)
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Poller,
    PollerFlag,
    Socket,
    SocketFactory,
    SocketType,
)
from exasol.analytics.utils.errors import UninitializedAttributeError

LOGGER: FilteringBoundLogger = structlog.get_logger()


def create_background_peer_state_builder() -> BackgroundPeerStateBuilder:
    timer_factory = TimerFactory()
    sender_factory = SenderFactory()
    connection_establisher_builder = ConnectionEstablisherBuilder(
        timer_factory=timer_factory
    )
    connection_closer_builder = ConnectionCloserBuilder(timer_factory=timer_factory)
    register_peer_forwarder_builder = RegisterPeerForwarderBuilder(
        timer_factory=timer_factory
    )
    payload_message_sender_factory = PayloadMessageSenderFactory(
        timer_factory=timer_factory
    )
    payload_sender_factory = PayloadSenderFactory(
        payload_message_sender_factory=payload_message_sender_factory
    )
    payload_handler_builder = PayloadHandlerBuilder(
        payload_sender_factory=payload_sender_factory
    )
    background_peer_state_factory = BackgroundPeerStateBuilder(
        sender_factory=sender_factory,
        connection_establisher_builder=connection_establisher_builder,
        connection_closer_builder=connection_closer_builder,
        register_peer_forwarder_builder=register_peer_forwarder_builder,
        payload_handler_builder=payload_handler_builder,
    )
    return background_peer_state_factory


@dataclasses.dataclass
class RuntimeSockets:
    in_control: Socket
    out_control: Socket
    listen: Socket
    listen_port: int
    poller: Poller

    @classmethod
    def create(
        cls,
        socket_factory: SocketFactory,
        name: str,
        in_control_address: str,
        out_control_address: str,
    ) -> "RuntimeSockets":
        def listen_socket():
            socket = socket_factory.create_socket(SocketType.ROUTER)
            socket.set_identity(name)
            port = socket.bind_to_random_port(f"tcp://*")
            return (socket, port)

        def control_socket(address: str):
            socket = socket_factory.create_socket(SocketType.PAIR)
            socket.connect(address)
            return socket

        in_control = control_socket(in_control_address)
        out_control = control_socket(out_control_address)
        listen, listen_port = listen_socket()

        poller = socket_factory.create_poller()
        poller.register(in_control, flags=PollerFlag.POLLIN)
        poller.register(listen, flags=PollerFlag.POLLIN)

        return cls(
            in_control,
            out_control,
            listen,
            listen_port,
            poller,
        )


class BackgroundListenerThread:
    class Status(enum.Enum):
        RUNNING = enum.auto()
        PREPARE_TO_STOP = enum.auto()
        STOPPED = enum.auto()

    def __init__(
        self,
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
        background_peer_state_factory: BackgroundPeerStateBuilder = create_background_peer_state_builder(),
    ):
        self._number_of_peers = number_of_peers
        self._config = config
        self._background_peer_state_factory = background_peer_state_factory
        self._trace_logging = trace_logging
        self._clock = clock
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=group_identifier,
            config=dataclasses.asdict(config),
        )
        self._group_identifier = group_identifier
        self._listen_ip = listen_ip
        self._in_control_socket_address = in_control_socket_address
        self._out_control_socket_address = out_control_socket_address
        self._socket_factory = socket_factory
        self._status = BackgroundListenerThread.Status.RUNNING
        self._peer_state: Dict[Peer, BackgroundPeerState] = {}
        self._register_peer_connection: Optional[RegisterPeerConnection] = None
        self._sockets: Optional[RuntimeSockets] = None

    def run(self):
        self._sockets = RuntimeSockets.create(
            self._socket_factory,
            self._name,
            self._in_control_socket_address,
            self._out_control_socket_address,
        )
        self._set_my_connection_info(self.sockets.listen_port)
        try:
            self._run_message_loop()
        finally:
            self._stop()

    def _stop(self):
        self._logger.info("start")
        if self._register_peer_connection is not None:
            self._register_peer_connection.close()
        self.sockets.out_control.close(linger=0)
        self.sockets.in_control.close(linger=0)
        self.sockets.listen.close(linger=0)
        self._logger.info("end")

    def _run_message_loop(self):
        try:
            while self._status != BackgroundListenerThread.Status.STOPPED:
                self._handle_message()
                self._try_send()
        except Exception as e:
            self._logger.exception("Exception in message loop")

    def _try_send(self):
        if self._status != BackgroundListenerThread.Status.STOPPED:
            for peer_state in self._peer_state.values():
                if self._status == BackgroundListenerThread.Status.PREPARE_TO_STOP:
                    peer_state.prepare_to_stop()
                peer_state.try_send()

    def _handle_message(self):
        poll = self.sockets.poller.poll(timeout_in_ms=self._config.poll_timeout_in_ms)
        if (
            self.sockets.in_control in poll
            and PollerFlag.POLLIN in poll[self.sockets.in_control]
        ):
            message = self.sockets.in_control.receive_multipart()
            self._status = self._handle_control_message(message)
        if (
            self.sockets.listen in poll
            and PollerFlag.POLLIN in poll[self.sockets.listen]
        ):
            message = self.sockets.listen.receive_multipart()
            self._handle_listener_message(message)

    def _handle_control_message(self, frames: List[Frame]) -> Status:
        try:
            message_obj: messages.Message = deserialize_message(
                frames[0].to_bytes(), messages.Message
            )
            specific_message_obj = message_obj.root
            if isinstance(specific_message_obj, messages.Stop):
                return BackgroundListenerThread.Status.STOPPED
            elif isinstance(specific_message_obj, PrepareToStop):
                return BackgroundListenerThread.Status.PREPARE_TO_STOP
            elif isinstance(specific_message_obj, messages.RegisterPeer):
                if self._is_register_peer_message_allowed_as_control_message():
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    self._logger.error(
                        "RegisterPeer message not allowed",
                        message_obj=specific_message_obj.model_dump(),
                    )
            elif isinstance(specific_message_obj, messages.Payload):
                self.send_payload(payload=specific_message_obj, frames=frames)
            else:
                self._logger.error(
                    "Unknown message type", message_obj=specific_message_obj.model_dump()
                )
        except Exception as e:
            self._logger.exception("Exception during handling message", message=frames)
        return self._status

    def _is_register_peer_message_allowed_as_control_message(self) -> bool:
        return (
            self._config.forward_register_peer_config.is_enabled
            and self._config.forward_register_peer_config.is_leader
        ) or not self._config.forward_register_peer_config.is_enabled

    def send_payload(self, payload: messages.Payload, frames: List[Frame]):
        self._peer_state[payload.destination].send_payload(
            message=payload, frames=frames
        )

    @property
    def sockets(self) -> RuntimeSockets:
        if self._sockets is None:
            raise UninitializedAttributeError("Runtime Sockets are not initialized.")
        return self._sockets

    def _add_peer(
        self,
        peer: Peer,
        register_peer_forwarder_behavior_config: RegisterPeerForwarderBehaviorConfig = RegisterPeerForwarderBehaviorConfig(),
    ):
        if (
            peer.connection_info.group_identifier
            != self._my_connection_info.group_identifier
        ):
            self._logger.error(
                "Peer belongs to a different group",
                my_connection_info=self._my_connection_info.model_dump(),
                peer=peer.model_dump(),
            )
            raise ValueError("Peer belongs to a different group")
        if peer not in self._peer_state:
            parameter = RegisterPeerForwarderBuilderParameter(
                register_peer_connection=self._register_peer_connection,
                timeout_config=self._config.register_peer_forwarder_timeout_config,
                behavior_config=register_peer_forwarder_behavior_config,
            )
            self._peer_state[peer] = self._background_peer_state_factory.create(
                my_connection_info=self._my_connection_info,
                peer=peer,
                out_control_socket=self.sockets.out_control,
                socket_factory=self._socket_factory,
                clock=self._clock,
                send_socket_linger_time_in_ms=self._config.send_socket_linger_time_in_ms,
                connection_establisher_timeout_config=self._config.connection_establisher_timeout_config,
                connection_closer_timeout_config=self._config.connection_closer_timeout_config,
                register_peer_forwarder_builder_parameter=parameter,
                payload_message_sender_timeout_config=self._config.payload_message_sender_timeout_config,
            )

    def _handle_listener_message(self, frames: List[Frame]):
        logger = self._logger.bind(sender_queue_id=frames[0].to_bytes())
        message_content_bytes = frames[1].to_bytes()
        try:
            message_obj: messages.Message = deserialize_message(
                message_content_bytes, messages.Message
            )
            specific_message_obj = message_obj.root
            if isinstance(specific_message_obj, messages.SynchronizeConnection):
                self._handle_synchronize_connection(specific_message_obj)
            elif isinstance(specific_message_obj, messages.AcknowledgeConnection):
                self._handle_acknowledge_connection(specific_message_obj)
            elif isinstance(specific_message_obj, messages.CloseConnection):
                self._handle_close_connection(specific_message_obj)
            elif isinstance(specific_message_obj, messages.AcknowledgeCloseConnection):
                self._handle_acknowledge_close_connection(specific_message_obj)
            elif isinstance(specific_message_obj, messages.RegisterPeer):
                if self.is_register_peer_message_allowed_as_listener_message():
                    self._handle_register_peer_message(specific_message_obj)
                else:
                    logger.error(
                        "RegisterPeer message not allowed",
                        message_obj=specific_message_obj.model_dump(),
                    )
            elif isinstance(specific_message_obj, messages.AcknowledgeRegisterPeer):
                self._handle_acknowledge_register_peer_message(specific_message_obj)
            elif isinstance(specific_message_obj, messages.RegisterPeerComplete):
                self._handle_register_peer_complete_message(specific_message_obj)
            elif isinstance(specific_message_obj, messages.Payload):
                self._handle_payload_message(specific_message_obj, frames[1:])
            elif isinstance(specific_message_obj, messages.AcknowledgePayload):
                self._handle_acknowledge_payload_message(specific_message_obj)
            else:
                logger.error(
                    "Unknown message type", message_obj=specific_message_obj.model_dump()
                )
        except Exception as e:
            logger.exception(
                "Exception during handling message",
                message_content=message_content_bytes,
            )

    def is_register_peer_message_allowed_as_listener_message(self) -> bool:
        return (
            not self._config.forward_register_peer_config.is_leader
            and self._config.forward_register_peer_config.is_enabled
        )

    def _handle_payload_message(self, payload: messages.Payload, frames: List[Frame]):
        self._peer_state[payload.source].received_payload(payload, frames=frames)

    def _handle_acknowledge_payload_message(
        self, acknowledge_payload: messages.AcknowledgePayload
    ):
        self._peer_state[acknowledge_payload.source].received_acknowledge_payload(
            acknowledge_payload
        )

    def _handle_synchronize_connection(self, message: messages.SynchronizeConnection):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_synchronize_connection()

    def _handle_acknowledge_connection(self, message: messages.AcknowledgeConnection):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_acknowledge_connection()

    def _handle_close_connection(self, message: messages.CloseConnection):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_close_connection()

    def _handle_acknowledge_close_connection(
        self, message: messages.AcknowledgeCloseConnection
    ):
        peer = Peer(connection_info=message.source)
        self._add_peer(peer)
        self._peer_state[peer].received_acknowledge_close_connection()

    def _set_my_connection_info(self, port: int):
        self._my_connection_info = ConnectionInfo(
            name=self._name,
            ipaddress=self._listen_ip,
            port=Port(port=port),
            group_identifier=self._group_identifier,
        )
        message = messages.MyConnectionInfo(my_connection_info=self._my_connection_info)
        self.sockets.out_control.send(serialize_message(message))

    def _handle_register_peer_message(self, message: messages.RegisterPeer):
        def config(needs_register: bool):
            needs_ack = not self._config.forward_register_peer_config.is_leader
            return RegisterPeerForwarderBehaviorConfig(
                needs_to_send_register_peer=needs_register,
                needs_to_send_acknowledge_register_peer=needs_ack,
            )

        if not self._config.forward_register_peer_config.is_enabled:
            self._add_peer(message.peer)
        elif self._register_peer_connection is None:
            self._register_peer_connection = self._create_register_peer_connection(message)
            self._add_peer(message.peer, config(needs_register=False))
        else:
            self._add_peer(message.peer, config(needs_register=True))

    def _create_register_peer_connection(self, message: messages.RegisterPeer) -> RegisterPeerConnection:
        successor_send_socket_factory = SendSocketFactory(
            my_connection_info=self._my_connection_info,
            peer=message.peer,
            socket_factory=self._socket_factory,
        )
        if message.source is not None:
            predecessor_send_socket_factory = SendSocketFactory(
                my_connection_info=self._my_connection_info,
                peer=message.source,
                socket_factory=self._socket_factory,
            )
        else:
            predecessor_send_socket_factory = None
        return RegisterPeerConnection(
            predecessor=message.source,
            predecessor_send_socket_factory=predecessor_send_socket_factory,
            successor=message.peer,
            successor_send_socket_factory=successor_send_socket_factory,
            my_connection_info=self._my_connection_info,
        )

    def _handle_acknowledge_register_peer_message(
        self, message: messages.AcknowledgeRegisterPeer
    ):
        if self._register_peer_connection is None:
            raise UninitializedAttributeError("Register peer connection is undefined.")
        if self._register_peer_connection.successor != message.source:
            self._logger.error(
                "AcknowledgeRegisterPeer message not from successor",
                message_obj=message.model_dump(),
            )
        peer = message.peer
        self._peer_state[peer].received_acknowledge_register_peer()

    def _handle_register_peer_complete_message(
        self, message: messages.RegisterPeerComplete
    ):
        if self._register_peer_connection is None:
            raise UninitializedAttributeError("Register peer connection is undefined.")
        if self._register_peer_connection.predecessor != message.source:
            self._logger.error(
                "RegisterPeerComplete message not from predecessor",
                message_obj=message.model_dump(),
            )
        peer = message.peer
        self._peer_state[peer].received_register_peer_complete()
