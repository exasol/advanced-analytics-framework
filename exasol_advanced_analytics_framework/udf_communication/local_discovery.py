import time
from typing import cast

from exasol_advanced_analytics_framework.udf_communication.ip_address import Port
from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.messages import PingMessage
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


class LocalDiscovery:

    def __init__(self,
                 discovery_timeout_in_minutes: int,
                 discovery_port: Port,
                 peer_communicator: PeerCommunicator,
                 local_discovery_socket: LocalDiscoverySocket
                 ):
        self._local_discovery_socket = local_discovery_socket
        self._peer_communicator = peer_communicator
        self._discovery_timeout_in_minutes = discovery_timeout_in_minutes
        self._discovery_port = discovery_port
        self._discover_peers()

    def _is_timeout(self, begin_time_ns: int):
        current_time_ns = time.monotonic_ns()
        time_difference_ns = begin_time_ns - current_time_ns
        discovery_timeout_in_ns = self._discovery_timeout_in_minutes * (60 * 10 ** 9)
        if time_difference_ns > discovery_timeout_in_ns:
            return True
        else:
            return False

    def _discover_peers(self):
        self._send_ping()
        begin_time_ns = time.monotonic_ns()
        while (self._peer_communicator.are_all_peers_connected()
               and not self._is_timeout(begin_time_ns)):
            self._receive_ping()
            self._send_ping()

    def _receive_ping(self):
        serialized_message = self._local_discovery_socket.recvfrom()
        if serialized_message is not None:
            ping_message = cast(PingMessage, deserialize_message(serialized_message, PingMessage))
            peer_connection_info = ping_message.connection_info
            self._peer_communicator.add_peer(peer_connection_info)

    def _send_ping(self):
        ping_message = PingMessage(
            connection_info=self._peer_communicator.my_connection_info
        )
        serialized_message = serialize_message(ping_message)
        self._local_discovery_socket.send(serialized_message)
