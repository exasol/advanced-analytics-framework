import socket
import time
from typing import cast

from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.messages import PingMessage
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


class LocalDiscovery:

    def __init__(self,
                 discovery_timeout_in_seconds: int,
                 peer_communicator: PeerCommunicator,
                 local_discovery_socket: LocalDiscoverySocket
                 ):
        self._local_discovery_socket = local_discovery_socket
        self._peer_communicator = peer_communicator
        self._discovery_timeout_in_minutes = discovery_timeout_in_seconds
        self._discover_peers()

    def _is_timeout(self, begin_time_ns: int):
        current_time_ns = time.monotonic_ns()
        time_difference_ns = current_time_ns - begin_time_ns
        discovery_timeout_in_ns = self._discovery_timeout_in_minutes * 10 ** 9
        result = time_difference_ns > discovery_timeout_in_ns
        return result

    def _discover_peers(self):
        self._send_ping()
        begin_time_ns = time.monotonic_ns()
        while not self._should_discovery_end(begin_time_ns):
            self._receive_ping()
            self._send_ping()

    def _should_discovery_end(self, begin_time_ns: int):
        result = self._peer_communicator.are_all_peers_connected() or self._is_timeout(begin_time_ns)
        return result

    def _receive_ping(self):
        try:
            serialized_message = self._local_discovery_socket.recvfrom()
        except socket.timeout as e:
            serialized_message = None
        if serialized_message is not None:
            ping_message = cast(PingMessage, deserialize_message(serialized_message, PingMessage))
            peer_connection_info = ping_message.source
            self._peer_communicator.register_peer(peer_connection_info)

    def _send_ping(self):
        ping_message = PingMessage(
            source=self._peer_communicator.my_connection_info
        )
        serialized_message = serialize_message(ping_message)
        self._local_discovery_socket.send(serialized_message)
