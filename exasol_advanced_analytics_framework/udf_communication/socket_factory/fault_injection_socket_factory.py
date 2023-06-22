from typing import Union, List, Set, Optional, Dict
from warnings import warn

import structlog
from numpy.random import RandomState
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import SocketType, \
    PollerFlag, Socket, Frame, Poller, SocketFactory

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class FIFrame(Frame):

    def __init__(self, internal_frame: Frame):
        self._internal_frame = internal_frame

    def to_bytes(self) -> bytes:
        return self._internal_frame.to_bytes()


class FISocket(Socket):

    def __init__(self, internal_socket: Socket, send_fault_probability: float, random_state: RandomState):
        self._random_state = random_state
        if not (send_fault_probability >= 0 and send_fault_probability < 1):
            raise ValueError(
                f"send_fault_probability needs to be between 0 and 1 (exclusive) was {send_fault_probability}.")
        self._logger = LOGGER.bind(
            module_name=__name__,
            clazz=self.__class__.__name__,
            socket=str(self)
        )
        self._send_fault_probability = send_fault_probability
        self._internal_socket = internal_socket
        self._is_inproc = False
        self._closed = False

    def _is_fault(self):
        random_sample = self._random_state.random_sample(1)[0].item()
        should_be_fault = random_sample < self._send_fault_probability
        is_fault = not self._is_inproc and should_be_fault
        return is_fault

    def send(self, message: bytes):
        if self._is_fault():
            self._logger.info("Fault injected", message=message)
            return
         self._internal_socket.send(message)


    def receive(self) -> bytes:
        message = self._internal_socket.receive()
        return message

    def receive_multipart(self) -> List[Frame]:
        message = self._internal_socket.receive_multipart()
        converted_message = [FIFrame(frame) for frame in message]
        return converted_message

    def send_multipart(self, message: List[Frame]):
        def convert_frame(frame: Frame):
            if not isinstance(frame, FIFrame):
                raise TypeError(f"Frame type not supported, {frame}")
            return frame._internal_frame
        if self._is_fault():
            self._logger.info("Fault injected", message=message)
            return
        converted_message = [convert_frame(frame) for frame in message]
        self._internal_socket.send_multipart(converted_message)

    def bind(self, address: str):
        if address.startswith("inproc"):
            self._is_inproc = True
        self._internal_socket.bind(address)

    def bind_to_random_port(self, address: str) -> int:
        self._is_inproc = address.startswith("inproc")
        return self._internal_socket.bind_to_random_port(address)

    def connect(self, address: str):
        self._is_inproc = address.startswith("inproc")
        self._internal_socket.connect(address)

    def poll(self,
             flags: Union[PollerFlag, Set[PollerFlag]],
             timeout_in_ms: Optional[int] = None) \
            -> Optional[Set[PollerFlag]]:
        return self._internal_socket.poll(flags, timeout_in_ms)

    def close(self, linger=None):
        self._internal_socket.close(linger=linger)
        self._closed = True

    def set_identity(self, name: str):
        self._internal_socket.set_identity(name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(linger=None)

    def __del__(self):
        if not self._closed:
            if warn is not None:
                # warn can be None during process teardown
                warn(
                    f"Unclosed socket {self}",
                    ResourceWarning,
                    stacklevel=2,
                    source=self,
                )
            self.close(linger=None)
        del self._internal_socket


class FIPoller(Poller):

    def __init__(self, internal_poller: Poller):
        self._internal_poller = internal_poller
        self._socket_map = {}

    def register(self, socket: Socket, flags: Union[PollerFlag, Set[PollerFlag]]) -> None:
        if not isinstance(socket, FISocket):
            raise TypeError(f"Socket type not supported {socket}")
        internal_socket = socket._internal_socket
        self._socket_map[internal_socket] = socket
        self._internal_poller.register(internal_socket, flags)

    def poll(self, timeout_in_ms: Optional[int] = None) -> Dict[Socket, Set[PollerFlag]]:
        poll_result = self._internal_poller.poll(timeout_in_ms)
        return {
            self._socket_map[internal_socket]: flags
            for internal_socket, flags in poll_result.items()
        }


class FISocketFactory(SocketFactory):

    def __init__(self, socket_factory: SocketFactory, send_fault_probability: float, random_state: RandomState):
        if not (send_fault_probability >= 0 and send_fault_probability < 1):
            raise ValueError(
                f"send_fault_probability needs to be between 0 and 1 (exclusive) was {send_fault_probability}.")
        self._send_fault_probability = send_fault_probability
        self._random_state = random_state
        self._socket_factory = socket_factory

    def create_socket(self, socket_type: SocketType) -> Socket:
        return FISocket(self._socket_factory.create_socket(socket_type), self._send_fault_probability,
                        self._random_state)

    def create_frame(self, message_part: bytes) -> Frame:
        return FIFrame(self._socket_factory.create_frame(message_part))

    def create_poller(self) -> Poller:
        return FIPoller(self._socket_factory.create_poller())
