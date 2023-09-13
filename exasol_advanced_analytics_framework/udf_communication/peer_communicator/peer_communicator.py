import time
from dataclasses import asdict
from typing import Optional, Dict, List, Callable

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.background_listener_interface import \
    BackgroundListenerInterface
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.clock import Clock
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.forward_register_peer_config import \
    ForwardRegisterPeerConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.frontend_peer_state import \
    FrontendPeerState
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator_config import \
    PeerCommunicatorConfig
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract \
    import SocketFactory, Frame

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
                 config: PeerCommunicatorConfig = PeerCommunicatorConfig(),
                 clock: Clock = Clock(),
                 trace_logging: bool = False):
        self._config = config
        self._socket_factory = socket_factory
        self._name = name
        self._group_identifier = group_identifier
        self._number_of_peers = number_of_peers
        self._logger = LOGGER.bind(
            name=self._name,
            group_identifier=self._group_identifier,
            number_of_peers=self._number_of_peers,
            config=asdict(config)
        )
        self._logger.info("init")
        self._background_listener = BackgroundListenerInterface(
            name=self._name,
            number_of_peers=number_of_peers,
            socket_factory=self._socket_factory,
            listen_ip=listen_ip,
            group_identifier=self._group_identifier,
            config=config,
            clock=clock,
            trace_logging=trace_logging,
        )
        self._my_connection_info = self._background_listener.my_connection_info
        self._logger = self._logger.bind(my_connection_info=self._my_connection_info.dict())
        self._logger.info("my_connection_info")
        self._peer_states: Dict[Peer, FrontendPeerState] = {}

    def _handle_messages(self, timeout_in_milliseconds: Optional[int] = 0):
        for message_obj, frames in self._background_listener.receive_messages(timeout_in_milliseconds):
            specific_message_obj = message_obj.__root__
            if isinstance(specific_message_obj, messages.ConnectionIsReady):
                peer = specific_message_obj.peer
                self._add_peer_state(peer)
                self._peer_states[peer].received_connection_is_ready()
            elif isinstance(specific_message_obj, messages.ConnectionIsClosed):
                peer = specific_message_obj.peer
                self._add_peer_state(peer)
                self._peer_states[peer].received_connection_is_closed()
            elif isinstance(specific_message_obj, messages.PeerRegisterForwarderIsReady):
                peer = specific_message_obj.peer
                self._add_peer_state(peer)
                self._peer_states[peer].received_peer_register_forwarder_is_ready()
            elif isinstance(specific_message_obj, messages.Timeout):
                raise TimeoutError(specific_message_obj.reason)
            elif isinstance(specific_message_obj, messages.Payload):
                self._peer_states[specific_message_obj.source].received_payload_message(specific_message_obj, frames)
            elif isinstance(specific_message_obj, messages.AcknowledgePayload):
                self._peer_states[specific_message_obj.source].received_acknowledge_payload_message(
                    specific_message_obj)
            elif isinstance(specific_message_obj, messages.AbortPayload):
                raise TimeoutError(specific_message_obj.reason)
            else:
                self._logger.error(
                    "Unknown message",
                    message_obj=specific_message_obj.dict())

    def _add_peer_state(self, peer: Peer):
        if peer not in self._peer_states:
            self._peer_states[peer] = FrontendPeerState(
                my_connection_info=self.my_connection_info,
                socket_factory=self._socket_factory,
                peer=peer,
                background_listener=self._background_listener
            )

    def _wait_for_condition(self, condition: Callable[[], bool],
                            timeout_in_milliseconds: Optional[int] = None) -> bool:
        start_time_ns = time.monotonic_ns()
        self._handle_messages(timeout_in_milliseconds=0)
        while not condition():
            if timeout_in_milliseconds is not None:
                handle_message_timeout_ms = _compute_handle_message_timeout(start_time_ns, timeout_in_milliseconds)
                if handle_message_timeout_ms < 0:
                    break
            else:
                handle_message_timeout_ms = None
            self._handle_messages(timeout_in_milliseconds=handle_message_timeout_ms)
        return condition()

    def wait_for_peers(self, timeout_in_milliseconds: Optional[int] = None) -> bool:
        return self._wait_for_condition(self._are_all_peers_connected, timeout_in_milliseconds)

    def peers(self, timeout_in_milliseconds: Optional[int] = None) -> Optional[List[Peer]]:
        self.wait_for_peers(timeout_in_milliseconds)
        if self._are_all_peers_connected():
            peers = [peer for peer in self._peer_states.keys()] + \
                    [Peer(connection_info=self._my_connection_info)]
            return sorted(peers, key=key_for_peer)
        else:
            return None

    def register_peer(self, peer_connection_info: ConnectionInfo):
        self._logger.info("register_peer", peer_connection_info=peer_connection_info.dict())
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

    @property
    def peer(self) -> Peer:
        return Peer(connection_info=self._my_connection_info)

    @property
    def rank(self) -> int:
        return self.peers().index(self.peer)

    @property
    def forward_register_peer_config(self) -> ForwardRegisterPeerConfig:
        return self._config.forward_register_peer_config

    def are_all_peers_connected(self) -> bool:
        self._handle_messages()
        result = self._are_all_peers_connected()
        return result

    def _are_all_peers_connected(self):
        all_peers_ready = all(peer_state.peer_is_ready for peer_state in self._peer_states.values())
        result = len(self._peer_states) == self._number_of_peers - 1 and all_peers_ready
        return result

    def send(self, peer: Peer, message: List[Frame]):
        self.wait_for_peers()
        self._peer_states[peer].send(message)

    def recv(self, peer: Peer, timeout_in_milliseconds: Optional[int] = None) -> List[Frame]:
        self.wait_for_peers()
        peer_has_received_messages = \
            self._wait_for_condition(self._peer_states[peer].has_received_messages,
                                     timeout_in_milliseconds=timeout_in_milliseconds)
        if peer_has_received_messages:
            return self._peer_states[peer].recv()
        else:
            raise TimeoutError("Timeout occurred during waiting for messages.")

    def poll_peers(
            self, peers: Optional[List[Peer]] = None,
            timeout_in_milliseconds: Optional[int] = None
    ) -> List[Peer]:
        self.wait_for_peers()

        if peers is None:
            peers = self._peer_states.keys()

        def have_peers_received_messages() -> bool:
            result = any(self._peer_states[peer].has_received_messages() for peer in peers)
            return result

        self._wait_for_condition(have_peers_received_messages,
                                 timeout_in_milliseconds=timeout_in_milliseconds)
        return [peer for peer in peers if self._peer_states[peer].has_received_messages()]

    def stop(self):
        self._logger.info("stop")
        if self._background_listener is not None:
            try:
                self._stop_background_listener()
            finally:
                self._remove_peer_states()

    def _stop_background_listener(self):
        self._logger.info("stop background_listener")
        self._background_listener.prepare_to_stop()
        try:
            is_ready_to_stop = \
                self._wait_for_condition(self._are_all_peers_disconnected,
                                         timeout_in_milliseconds=self._config.close_timeout_in_ms)
            if not is_ready_to_stop:
                raise TimeoutError("Timeout expired, could not gracefully stop PeerCommuincator.")
        finally:
            self._background_listener.stop()
            self._background_listener = None

    def _are_all_peers_disconnected(self):
        all_peers_ready = all(peer_state.connection_is_closed for peer_state in self._peer_states.values())
        result = len(self._peer_states) == self._number_of_peers - 1 and all_peers_ready
        return result

    def _remove_peer_states(self):
        self._logger.info("stop peer_states")
        for peer_state_key in list(self._peer_states.keys()):
            del self._peer_states[peer_state_key]

    def __del__(self):
        self.stop()
