import time
from typing import cast

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port
from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize, deserialize


class PingMessage(BaseModel):
    connection_info: ConnectionInfo


class LocalDiscovery:

    def __init__(self,
                 discovery_timeout_in_minutes: int,
                 discovery_port: Port,
                 peer_communicator: PeerCommunicator):
        self._peer_communicator = peer_communicator
        self._discovery_timeout_in_minutes = discovery_timeout_in_minutes
        self._discovery_port = discovery_port

    def _is_timeout(self, begin_time_ns: int):
        current_time_ns = time.monotonic_ns()
        time_difference_ns = begin_time_ns - current_time_ns
        discovery_timeout_in_ns = self._discovery_timeout_in_minutes * (60 * 10 ** 9)
        if time_difference_ns > discovery_timeout_in_ns:
            return True
        else:
            return False

    def discover_peers(self):
        socket = LocalDiscoverySocket(self._discovery_port, timeout=5)
        self.send_ping(socket)
        begin_time_ns = time.monotonic_ns()
        while (self._peer_communicator.are_all_peers_connected()
               and not self._is_timeout(begin_time_ns)):
            self.receive_ping(socket)
            self.send_ping(socket)

    def receive_ping(self, socket):
        serialized_message = socket.recvfrom()
        if serialized_message is not None:
            ping_message = cast(PingMessage, deserialize(serialized_message, PingMessage))
            peer_connection_info = ping_message.connection_info
            self._peer_communicator.add_peer(peer_connection_info)

    def send_ping(self, socket):
        ping_message = PingMessage(
            connection_info=self._peer_communicator.get_my_connection_info()
        )
        serialized_message = serialize(ping_message)
        socket.send(serialized_message)
