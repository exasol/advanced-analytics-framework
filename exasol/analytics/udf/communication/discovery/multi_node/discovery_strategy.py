import socket
import time
from typing import (
    Optional,
    cast,
)

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.discovery.multi_node.discovery_socket import (
    DiscoverySocketFactory,
)
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator import (
    PeerCommunicator,
)
from exasol.analytics.udf.communication.serialization import (
    deserialize_message,
    serialize_message,
)

NANOSECONDS_PER_SECOND = 10**9


def _to_ping_message(serialized_message: bytes) -> messages.Ping:
    ping_message = cast(
        messages.Ping, deserialize_message(serialized_message, messages.Ping)
    )
    return ping_message


class DiscoveryStrategy:

    def __init__(
        self,
        ip_address: IPAddress,
        port: Port,
        timeout_in_seconds: int,
        time_between_ping_messages_in_seconds: float,
        peer_communicator: PeerCommunicator,
        global_discovery_socket_factory: DiscoverySocketFactory,
    ):
        self._peer_communicator = peer_communicator
        self._time_between_ping_messages_in_seconds = float(
            time_between_ping_messages_in_seconds
        )
        self._global_discovery_socket = global_discovery_socket_factory.create(
            ip_address=ip_address, port=port
        )
        self._timeout_in_ns = timeout_in_seconds * NANOSECONDS_PER_SECOND

    def _has_discovery_timed_out(self, begin_time_ns: int) -> bool:
        time_left_until_timeout = self._time_left_until_discovery_timeout_in_ns(
            begin_time_ns
        )
        return time_left_until_timeout == 0

    def _time_left_until_discovery_timeout_in_ns(self, begin_time_ns: int) -> int:
        current_time_ns = time.monotonic_ns()
        time_difference_ns = current_time_ns - begin_time_ns
        time_left_until_timeout = self._timeout_in_ns - time_difference_ns
        return max(0, time_left_until_timeout)

    def discover_peers(self):
        if not self._peer_communicator.forward_register_peer_config.is_enabled:
            raise ValueError(
                "PeerCommunicator.is_forward_register_peer_enabled needs to be true"
            )
        if self._peer_communicator.forward_register_peer_config.is_leader:
            self._global_discovery_socket.bind()
        self._send_ping()
        begin_time_ns = time.monotonic_ns()
        while not self._should_discovery_end(begin_time_ns):
            if self._peer_communicator.forward_register_peer_config.is_leader:
                self._receive_pings(begin_time_ns)
            self._send_ping()
        if not self._peer_communicator.are_all_peers_connected():
            raise TimeoutError()

    def _should_discovery_end(self, begin_time_ns: int) -> bool:
        are_all_peers_connected = self._peer_communicator.are_all_peers_connected()
        has_discovery_timed_out = self._has_discovery_timed_out(begin_time_ns)
        result = are_all_peers_connected or has_discovery_timed_out
        return result

    def _receive_pings(self, begin_time_ns: int):
        timeout_in_seconds = self._compute_receive_timeout_in_seconds(begin_time_ns)
        while not self._peer_communicator.are_all_peers_connected():
            serialized_message = self._receive_message(timeout_in_seconds)
            if serialized_message is None:
                break
            timeout_in_seconds = self._handle_serialized_message(serialized_message)

    def _compute_receive_timeout_in_seconds(self, begin_time_ns: int) -> float:
        time_left_until_timeout_in_seconds = (
            self._time_left_until_discovery_timeout_in_ns(begin_time_ns)
            / NANOSECONDS_PER_SECOND
        )
        timeout_in_seconds = min(
            time_left_until_timeout_in_seconds,
            self._time_between_ping_messages_in_seconds,
        )
        return timeout_in_seconds

    def _handle_serialized_message(self, serialized_message) -> float:
        TIMEOUT_IN_SECONDS = 0.0
        ping_message = _to_ping_message(serialized_message)
        if ping_message is not None:
            self._peer_communicator.register_peer(ping_message.source)
        return TIMEOUT_IN_SECONDS

    def _receive_message(self, timeout_in_seconds: float) -> Optional[bytes]:
        try:
            serialized_message = self._global_discovery_socket.recvfrom(
                timeout_in_seconds=timeout_in_seconds
            )
        except socket.timeout as e:
            serialized_message = None
        return serialized_message

    def _send_ping(self):
        ping_message = messages.Ping(source=self._peer_communicator.my_connection_info)
        serialized_message = serialize_message(ping_message)
        self._global_discovery_socket.send(serialized_message)
