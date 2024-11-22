import threading
from dataclasses import asdict, dataclass
from typing import Iterator, List, Optional, Set, Tuple

import structlog
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
from exasol.analytics.udf.communication.messages import (
    IsReadyToStop,
    Message,
    PrepareToStop,
    Stop,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.background_listener_thread import (
    BackgroundListenerThread,
)
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.serialization import (
    deserialize_message,
    serialize_message,
)
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    PollerFlag,
    Socket,
    SocketFactory,
    SocketType,
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


class UnexpectedMessageError(Exception):
    """
    BackgroundListenerInterface received a message of an unexpected type
    instead of messages.MyConnectionInfo.
    """


@dataclass
class SocketWithAddress:
    socket: Socket
    address: str

    @classmethod
    def create(self, socket_factory: SocketFactory, name: str) -> "SocketWithAddress":
        socket = socket_factory.create_socket(SocketType.PAIR)
        address = f"inproc://BackgroundListener_{name}"
        socket.bind(address)
        return SocketWithAddress(socket, address)


class BackgroundListenerInterface:

    def __init__(
        self,
        name: str,
        number_of_peers: int,
        socket_factory: SocketFactory,
        listen_ip: IPAddress,
        group_identifier: str,
        config: PeerCommunicatorConfig,
        clock: Clock,
        trace_logging: bool,
    ):
        self._socket_factory = socket_factory
        self._config = config
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name, group_identifier=group_identifier, config=asdict(config)
        )
        self._out_control = SocketWithAddress.create(
            socket_factory, f"out_control_socket{id(self)}"
        )
        self._in_control = SocketWithAddress.create(
            socket_factory, f"in_control_socket{id(self)}"
        )
        self._is_ready_to_stop = False
        self._background_listener_run = BackgroundListenerThread(
            name=self._name,
            number_of_peers=number_of_peers,
            socket_factory=socket_factory,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            out_control_socket_address=self._out_control.address,
            in_control_socket_address=self._in_control.address,
            clock=clock,
            config=config,
            trace_logging=trace_logging,
        )
        self._thread = threading.Thread(target=self._background_listener_run.run)
        self._thread.daemon = True
        self._thread.start()
        self._my_connection_info = self._get_my_connection_info()

    def _get_my_connection_info(self) -> ConnectionInfo:
        received = None
        try:
            received = self._out_control.socket.receive()
            generic = deserialize_message(received, messages.Message)
            message = generic.__root__
            if not isinstance(message, messages.MyConnectionInfo):
                raise UnexpectedMessageError(
                    f"Unexpected message of type {type(message)}"
                )
            return message.my_connection_info
        except Exception as e:
            self._logger.exception("Exception", raw_message=received)
            raise

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def register_peer(self, peer: Peer):
        register_message = messages.RegisterPeer(peer=peer)
        self._in_control.socket.send(serialize_message(register_message))

    def send_payload(self, message: messages.Payload, payload: List[Frame]):
        serialized_message = serialize_message(message)
        frame = self._socket_factory.create_frame(serialized_message)
        self._in_control.socket.send_multipart([frame] + payload)

    def receive_messages(
        self, timeout_in_milliseconds: Optional[int] = 0
    ) -> Iterator[Tuple[Message, List[Frame]]]:
        def poll() -> Set[PollerFlag]:
            return (
                self._out_control.socket.poll(
                    flags=PollerFlag.POLLIN,
                    timeout_in_ms=timeout_in_milliseconds,
                )
                or set()
            )

        while PollerFlag.POLLIN in poll():
            message = None
            try:
                timeout_in_milliseconds = 0
                frames = self._out_control.socket.receive_multipart()
                message_obj: Message = deserialize_message(
                    frames[0].to_bytes(), Message
                )
                yield message_obj, frames
            except Exception as e:
                self._logger.exception("Exception", raw_message=message)

    def stop(self):
        self._logger.info("start")
        self._send_stop()
        self._thread.join()
        self._out_control.socket.close(linger=0)
        self._in_control.socket.close(linger=0)
        self._logger.info("end")

    def _send_stop(self):
        self._in_control.socket.send(serialize_message(Stop()))

    def prepare_to_stop(self):
        self._logger.info("start")
        self._send_prepare_to_stop()
        self._logger.info("end")

    def _send_prepare_to_stop(self):
        self._in_control.socket.send(serialize_message(PrepareToStop()))
