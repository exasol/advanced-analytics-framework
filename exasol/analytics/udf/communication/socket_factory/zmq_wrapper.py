from typing import Dict, List, Optional, Set, Union
from warnings import warn

import zmq

from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Poller,
    PollerFlag,
    Socket,
    SocketFactory,
    SocketType,
)


def _flags_to_bitmask(flags: Union[PollerFlag, Set[PollerFlag]]) -> int:
    if isinstance(flags, set):
        result = 0
        for flag in flags:
            bitmask_for_flag = _flags_to_bitmask(flag)
            result |= bitmask_for_flag
        return result
    elif isinstance(flags, PollerFlag):
        if flags == PollerFlag.POLLIN:
            return zmq.POLLIN
        elif flags == PollerFlag.POLLOUT:
            return zmq.POLLOUT
    else:
        raise ValueError(f"Flag not supported {flags}")


def _bitmask_to_flags(bitmask: int) -> Set[PollerFlag]:
    result = set()
    if bitmask & zmq.POLLIN != 0:
        result.add(PollerFlag.POLLIN)
    if bitmask & zmq.POLLOUT != 0:
        result.add(PollerFlag.POLLOUT)
    return result


class ZMQFrame(Frame):

    def __init__(self, internal_frame: zmq.Frame):
        self._internal_frame = internal_frame

    def to_bytes(self) -> bytes:
        return self._internal_frame.bytes


class ZMQSocket(Socket):

    def __init__(self, internal_socket: zmq.Socket):
        self._internal_socket = internal_socket
        self._closed = False

    def send(self, message: bytes):
        self._internal_socket.send(message)

    def receive(self) -> bytes:
        return self._internal_socket.recv()

    def receive_multipart(self) -> List[Frame]:
        def convert_frame(frame: zmq.Frame):
            if not isinstance(frame, zmq.Frame):
                raise ValueError(f"Frame not supported {frame}")
            return ZMQFrame(frame)

        zmq_message = self._internal_socket.recv_multipart(copy=False)
        message = [convert_frame(frame) for frame in zmq_message]
        return message

    def send_multipart(self, message: List[Frame]):
        def convert_frame(frame: Frame):
            if not isinstance(frame, ZMQFrame):
                raise ValueError(f"Frame not supported {frame}")
            return frame._internal_frame

        zmq_message = [convert_frame(frame) for frame in message]
        return self._internal_socket.send_multipart(zmq_message, copy=False)

    def bind(self, address: str):
        self._internal_socket.bind(address)

    def bind_to_random_port(self, address: str) -> int:
        return self._internal_socket.bind_to_random_port(address)

    def connect(self, address: str):
        self._internal_socket.connect(address)

    def poll(
        self,
        flags: Union[PollerFlag, Set[PollerFlag]],
        timeout_in_ms: Optional[int] = None,
    ) -> Set[PollerFlag]:
        input_bitmask = _flags_to_bitmask(flags)
        result_bitmask = self._internal_socket.poll(
            flags=input_bitmask, timeout=timeout_in_ms
        )
        result_set = _bitmask_to_flags(result_bitmask)
        return result_set

    def close(self, linger=None):
        self._internal_socket.close(linger=linger)
        self._closed = True

    def set_identity(self, name: str):
        self._internal_socket.setsockopt_string(zmq.IDENTITY, name)

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


class ZMQPoller(Poller):
    def __init__(self):
        self._internal_poller = zmq.Poller()
        self._sockets_map = {}

    def register(
        self, socket: Socket, flags: Union[PollerFlag, Set[PollerFlag]]
    ) -> None:
        if isinstance(socket, ZMQSocket):
            self._sockets_map[socket._internal_socket] = socket
            bitmask = _flags_to_bitmask(flags)
            self._internal_poller.register(socket._internal_socket, flags=bitmask)
        else:
            raise ValueError(f"Socket not supported: {socket}")

    def poll(
        self, timeout_in_ms: Optional[int] = None
    ) -> Dict[Socket, Set[PollerFlag]]:
        poll_result = dict(self._internal_poller.poll(timeout_in_ms))
        result = {
            self._sockets_map[zmq_socket]: _bitmask_to_flags(bitmask)
            for zmq_socket, bitmask in poll_result.items()
        }
        return result  # type: ignore


class ZMQSocketFactory(SocketFactory):

    def __init__(self, context: zmq.Context):
        self._context = context

    def create_socket(self, socket_type: SocketType) -> Socket:
        if socket_type == SocketType.PAIR:
            zmq_socket_type = zmq.PAIR
        elif socket_type == SocketType.DEALER:
            zmq_socket_type = zmq.DEALER
        elif socket_type == SocketType.ROUTER:
            zmq_socket_type = zmq.ROUTER
        else:
            raise ValueError(f"Unknown socket_type {socket_type}")
        return ZMQSocket(self._context.socket(zmq_socket_type))

    def create_frame(self, message_part: bytes) -> Frame:
        return ZMQFrame(zmq.Frame(message_part))

    def create_poller(self) -> Poller:
        return ZMQPoller()
