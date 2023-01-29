import time
from typing import Optional, Dict, List

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import PeerIsReadyToReceiveMessage, TimeoutMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_interface import \
    BackgroundListenerInterface
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.frontend_peer_state import \
    FrontendPeerState
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, \
    Frame

LOGGER: FilteringBoundLogger = structlog.getLogger()


def key_for_peer(peer: Peer):
    return peer.connection_info.ipaddress.ip_address + "_" + str(peer.connection_info.port.port)


def _compute_handle_message_timeout(start_time_ns: int, timeout_in_milliseconds: Optional[int] = None) -> int:
    time_difference_ns = time.monotonic_ns() - start_time_ns
    time_difference_ms = time_difference_ns // 10 ** 6
    handle_message_timeout_ms = timeout_in_milliseconds - time_difference_ms
    return handle_message_timeout_ms


class PeerCommunicator:

    def __init__(self,
                 name: str,
                 number_of_peers: int,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 socket_factory: SocketFactory,
                 leader: bool = False,
                 forward: bool = False,
                 poll_timeout_in_ms: int = 500,
                 synchronize_timeout_in_ms: int = 1000,
                 abort_timeout_in_ms: int = 240000,
                 peer_is_ready_wait_time_in_ms: int = 10000,
                 send_socket_linger_time_in_ms: int = 100,
                 clock: Clock = Clock(),
                 trace_logging: bool = False):
        self._socket_factory = socket_factory
        self._name = name
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=group_identifier
        )
        self._number_of_peers = number_of_peers
        self._background_listener = BackgroundListenerInterface(
            name=self._name,
            socket_factory=self._socket_factory,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            leader=leader,
            forward=forward,
            clock=clock,
            poll_timeout_in_ms=poll_timeout_in_ms,
            synchronize_timeout_in_ms=synchronize_timeout_in_ms,
            abort_timeout_in_ms=abort_timeout_in_ms,
            peer_is_ready_wait_time_in_ms=peer_is_ready_wait_time_in_ms,
            send_socket_linger_time_in_ms=send_socket_linger_time_in_ms,
            trace_logging=trace_logging
        )
        self._my_connection_info = self._background_listener.my_connection_info
        self._peer_states: Dict[Peer, FrontendPeerState] = {}

    def _handle_messages(self, timeout_in_milliseconds: Optional[int] = 0):
        if self._are_all_peers_connected():
            return

        for message in self._background_listener.receive_messages(timeout_in_milliseconds):
            if isinstance(message, PeerIsReadyToReceiveMessage):
                peer = message.peer
                self._add_peer_state(peer)
                self._peer_states[peer].received_peer_is_ready_to_receive()
            elif isinstance(message, TimeoutMessage):
                raise TimeoutError()
            else:
                self._logger.error("Unknown message", message=message.dict())

    def _add_peer_state(self, peer):
        if peer not in self._peer_states:
            self._peer_states[peer] = FrontendPeerState(
                my_connection_info=self.my_connection_info,
                socket_factory=self._socket_factory,
                peer=peer
            )

    def wait_for_peers(self, timeout_in_milliseconds: Optional[int] = None) -> bool:
        start_time_ns = time.monotonic_ns()
        while True:
            if timeout_in_milliseconds is not None:
                handle_message_timeout_ms = _compute_handle_message_timeout(start_time_ns, timeout_in_milliseconds)
                if handle_message_timeout_ms < 0:
                    break
            else:
                handle_message_timeout_ms = None
            self._handle_messages(timeout_in_milliseconds=handle_message_timeout_ms)
            if self._are_all_peers_connected():
                break
        connected = self._are_all_peers_connected()
        return connected

    def peers(self, timeout_in_milliseconds: Optional[int] = None) -> Optional[List[Peer]]:
        self.wait_for_peers(timeout_in_milliseconds)
        if self._are_all_peers_connected():
            peers = [peer for peer in self._peer_states.keys()]
            return sorted(peers, key=key_for_peer)
        else:
            return None

    def register_peer(self, peer_connection_info: ConnectionInfo):
        self._handle_messages()
        if (peer_connection_info.group_identifier == self.my_connection_info.group_identifier
                and peer_connection_info != self.my_connection_info):
            peer = Peer(connection_info=peer_connection_info)
            if peer not in self._peer_states:
                self._add_peer_state(peer)
                self._background_listener.register_peer(peer)
                self._handle_messages()

    @property
    def my_connection_info(self) -> ConnectionInfo:
        return self._my_connection_info

    def are_all_peers_connected(self) -> bool:
        self._handle_messages()
        result = self._are_all_peers_connected()
        return result

    def _are_all_peers_connected(self):
        all_peers_ready = all(peer_state.peer_is_ready for peer_state in self._peer_states.values())
        result = len(self._peer_states) == self._number_of_peers - 1 and all_peers_ready
        return result

    def send(self, peer: Peer, message: List[Frame]):
        assert self.are_all_peers_connected()
        self._peer_states[peer].send(message)

    def recv(self, peer: Peer, timeout_in_milliseconds: Optional[int] = None) -> List[Frame]:
        assert self.are_all_peers_connected()
        return self._peer_states[peer].recv(timeout_in_milliseconds)

    def close(self):
        self._logger.info("close")
        if self._background_listener is not None:
            self._background_listener.close()
            self._background_listener = None
        for peer_state in self._peer_states.values():
            peer_state.close()

    def __del__(self):
        self.close()
