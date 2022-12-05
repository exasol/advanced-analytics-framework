import abc
from enum import Enum, auto
from typing import Union, List, Set, Optional, Dict


class Frame(abc.ABC):
    """
    Abstraction for a memory buffer which can exchanged between sockets without moving it to Python
    """

    @abc.abstractmethod
    def to_bytes(self) -> bytes:
        """Copies the memory buffer to Python"""
        pass


class PollerFlag(Enum):
    POLLIN = auto()
    POLLOUT = auto()


class Socket(abc.ABC):
    @abc.abstractmethod
    def send(self, message: bytes):
        """Sends a message asynchronously if output queue has space, otherwise blocks"""

    @abc.abstractmethod
    def receive(self) -> bytes:
        """Receive a message synchronously"""

    @abc.abstractmethod
    def receive_multipart(self) -> List[Frame]:
        """Receive a multipart message synchronously"""

    @abc.abstractmethod
    def send_multipart(self, message: List[Frame]):
        """Sends a multipart message asynchronously"""

    @abc.abstractmethod
    def bind(self, address: str):
        """Bind to the given address"""

    @abc.abstractmethod
    def bind_to_random_port(self, address: str) -> int:
        """Bind to the given address with a random port and return the port"""

    @abc.abstractmethod
    def connect(self, address: str):
        """Connect to the given address"""

    @abc.abstractmethod
    def poll(self,
             flags: Union[PollerFlag, Set[PollerFlag]],
             timeout_in_ms: Optional[int] = None) \
            -> Optional[Set[PollerFlag]]:
        """
        Checks if the socket can receive or send without blocking or
        if timeout is set, it waits until a requested event occurred.
        """

    @abc.abstractmethod
    def close(self, linger: int = None):
        """
        Closes the socket asynchronously. but waits until no unsent messages are queued.
        If linger is not None it forces the close after the number of seconds.
        """

    @abc.abstractmethod
    def set_identity(self, name: str):
        """
        Sets the identity of the socket.
        """

    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Poller(abc.ABC):

    @abc.abstractmethod
    def register(self, socket: Socket, flags: Union[PollerFlag, Set[PollerFlag]]) -> None:
        """Register a socket with the events we want to poll."""

    @abc.abstractmethod
    def poll(self, timeout_in_ms: Optional[int] = None) -> Dict[Socket, Set[PollerFlag]]:
        """Poll if an event occurred for the registered sockets or wait until an event occurred, if timeout is set. """


class SocketType(Enum):
    PAIR = auto()
    ROUTER = auto()
    DEALER = auto()


class SocketFactory(abc.ABC):

    @abc.abstractmethod
    def create_socket(self, socket_type: SocketType) -> Socket:
        pass

    @abc.abstractmethod
    def create_frame(self, message_part: bytes) -> Frame:
        pass

    @abc.abstractmethod
    def create_poller(self) -> Poller:
        pass
