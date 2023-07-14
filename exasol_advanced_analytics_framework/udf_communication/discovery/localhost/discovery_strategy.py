import socket
import time
from typing import cast, Optional

from exasol_advanced_analytics_framework.udf_communication.discovery.localhost import DiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message
from tests.udf_communication.test_messages import messages

NANOSECONDS_PER_SECOND = 10 ** 9


def _convert_to_ping_message(serialized_message: bytes) -> messages.Ping:
    ping_message = cast(messages.Ping, deserialize_message(serialized_message, messages.Ping))
    return ping_message


class DiscoveryStrategy:

    def __init__(self,
                 discovery_timeout_in_seconds: int,
                 time_between_ping_messages_in_seconds: float,
                 peer_communicator: PeerCommunicator,
                 discovery_socket: DiscoverySocket
                 ):
        self._time_between_ping_messages_in_seconds = float(time_between_ping_messages_in_seconds)
        self._local_discovery_socket = discovery_socket
        self._peer_communicator = peer_communicator
        self._discovery_timeout_in_ns = discovery_timeout_in_seconds * NANOSECONDS_PER_SECOND
        self._discover_peers()

    def _has_discovery_timed_out(self, begin_time_ns: int) -> bool:
        time_left_until_timeout = self._time_left_until_discovery_timeout_in_ns(begin_time_ns)
        return time_left_until_timeout == 0

    def _time_left_until_discovery_timeout_in_ns(self, begin_time_ns: int) -> int:
        current_time_ns = time.monotonic_ns()
        time_difference_ns = current_time_ns - begin_time_ns
        time_left_until_timeout = self._discovery_timeout_in_ns - time_difference_ns
        return max(0, time_left_until_timeout)

    def _discover_peers(self):
        self._send_ping()
        begin_time_ns = time.monotonic_ns()
        while not self._should_discovery_end(begin_time_ns):
            self._receive_pings(begin_time_ns)
            self._send_ping()

    def _should_discovery_end(self, begin_time_ns: int) -> bool:
        result = self._peer_communicator.are_all_peers_connected() or self._has_discovery_timed_out(begin_time_ns)
        return result

    def _receive_pings(self, begin_time_ns: int):
        timeout_in_seconds = self._compute_receive_timeout_in_seconds(begin_time_ns)
        while True:
            serialized_message = self._receive_message(timeout_in_seconds)
            if serialized_message is not None:
                timeout_in_seconds = self.handle_serialized_message(serialized_message)
                if self._peer_communicator.are_all_peers_connected():
                    break
            else:
                break

    def _compute_receive_timeout_in_seconds(self, begin_time_ns: int) -> float:
        time_left_until_timeout_in_seconds = \
            self._time_left_until_discovery_timeout_in_ns(begin_time_ns) / NANOSECONDS_PER_SECOND
        timeout_in_seconds = min(time_left_until_timeout_in_seconds,
                                 self._time_between_ping_messages_in_seconds)
        return timeout_in_seconds

    def handle_serialized_message(self, serialized_message) -> float:
        ping_message = _convert_to_ping_message(serialized_message)
        timeout_in_seconds = 0.0
        if ping_message is not None:
            self._peer_communicator.register_peer(ping_message.source)
        return timeout_in_seconds

    def _receive_message(self, timeout_in_seconds: float) -> Optional[bytes]:
        try:
            serialized_message = \
                self._local_discovery_socket.recvfrom(timeout_in_seconds=timeout_in_seconds)
        except socket.timeout as e:
            serialized_message = None
        return serialized_message

    def _send_ping(self):
        ping_message = messages.Ping(
            source=self._peer_communicator.my_connection_info
        )
        serialized_message = serialize_message(ping_message)
        self._local_discovery_socket.send(serialized_message)
