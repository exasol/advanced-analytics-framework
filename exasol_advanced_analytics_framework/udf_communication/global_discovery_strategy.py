import socket
import time
from typing import cast, Optional

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.global_discovery_socket import GlobalDiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.messages import PingMessage
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message

NANOSECONDS_PER_SECOND = 10 ** 9


def _convert_to_ping_message(serialized_message: bytes) -> PingMessage:
    ping_message = cast(PingMessage, deserialize_message(serialized_message, PingMessage))
    return ping_message


class GlobalDiscoveryStrategy:

    def __init__(self,
                 discovery_timeout_in_seconds: int,
                 time_between_ping_messages_in_seconds: float,
                 global_discovery_socket: GlobalDiscoverySocket):
        self._time_between_ping_messages_in_seconds = float(time_between_ping_messages_in_seconds)
        self._global_discovery_socket = global_discovery_socket
        self._discovery_timeout_in_ns = discovery_timeout_in_seconds * NANOSECONDS_PER_SECOND

    def _has_discovery_timed_out(self, begin_time_ns: int) -> bool:
        time_left_until_timeout = self._time_left_until_discovery_timeout_in_ns(begin_time_ns)
        return time_left_until_timeout == 0

    def _time_left_until_discovery_timeout_in_ns(self, begin_time_ns: int) -> int:
        current_time_ns = time.monotonic_ns()
        time_difference_ns = current_time_ns - begin_time_ns
        time_left_until_timeout = self._discovery_timeout_in_ns - time_difference_ns
        return max(0, time_left_until_timeout)

    def discover_peers(self, peer_communicator: PeerCommunicator):
        if not peer_communicator.is_forward_register_peer_enabled:
            raise ValueError("PeerCommunicator.is_forward_register_peer_enabled needs to be true")
        if peer_communicator.is_forward_register_peer_leader:
            self._global_discovery_socket.bind()
        self._send_ping(peer_communicator.my_connection_info)
        begin_time_ns = time.monotonic_ns()
        while not self._should_discovery_end(begin_time_ns, peer_communicator):
            if peer_communicator.is_forward_register_peer_leader:
                self._receive_pings(begin_time_ns, peer_communicator)
            self._send_ping(peer_communicator.my_connection_info)

    def _should_discovery_end(self, begin_time_ns: int, peer_communicator: PeerCommunicator) -> bool:
        are_all_peers_connected = peer_communicator.are_all_peers_connected()
        has_discovery_timed_out = self._has_discovery_timed_out(begin_time_ns)
        result = are_all_peers_connected or has_discovery_timed_out
        return result

    def _receive_pings(self, begin_time_ns: int, peer_communicator: PeerCommunicator):
        timeout_in_seconds = self._compute_receive_timeout_in_seconds(begin_time_ns)
        while True:
            serialized_message = self._receive_message(timeout_in_seconds)
            if serialized_message is not None:
                timeout_in_seconds = self._handle_serialized_message(serialized_message, peer_communicator)
                if peer_communicator.are_all_peers_connected():
                    break
            else:
                break

    def _compute_receive_timeout_in_seconds(self, begin_time_ns: int) -> float:
        time_left_until_timeout_in_seconds = \
            self._time_left_until_discovery_timeout_in_ns(begin_time_ns) / NANOSECONDS_PER_SECOND
        timeout_in_seconds = min(time_left_until_timeout_in_seconds,
                                 self._time_between_ping_messages_in_seconds)
        return timeout_in_seconds

    def _handle_serialized_message(self, serialized_message: bytes, peer_communicator: PeerCommunicator) -> float:
        ping_message = _convert_to_ping_message(serialized_message)
        timeout_in_seconds = 0.0
        if ping_message is not None:
            peer_communicator.register_peer(ping_message.source)
        return timeout_in_seconds

    def _receive_message(self, timeout_in_seconds: float) -> Optional[bytes]:
        try:
            serialized_message = \
                self._global_discovery_socket.recvfrom(timeout_in_seconds=timeout_in_seconds)
        except socket.timeout as e:
            serialized_message = None
        return serialized_message

    def _send_ping(self, my_connection_info: ConnectionInfo):
        ping_message = PingMessage(
            source=my_connection_info
        )
        serialized_message = serialize_message(ping_message)
        self._global_discovery_socket.send(serialized_message)
