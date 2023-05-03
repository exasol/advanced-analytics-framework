from dataclasses import asdict

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import PeerRegisterForwarderIsReadyMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.connection_establisher_behavior_config import \
    RegisterPeerForwarderBehaviorConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class RegisterPeerForwarderIsReadySender:

    def __init__(self,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 timer: Timer,
                 out_control_socket: Socket,
                 behavior_config: RegisterPeerForwarderBehaviorConfig):
        self._behavior_config = behavior_config
        self._peer = peer
        self._received_acknowledge_register_peer = False
        self._received_register_peer_complete = False
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._finished = False
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict(),
            behavior_config=asdict(self._behavior_config))

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._received_acknowledge_register_peer = True
        self._timer.reset_timer()

    def received_register_peer_complete(self):
        self._logger.debug("received_register_peer_complete")
        self._received_register_peer_complete = True

    def try_send(self):
        self._logger.info("RegisterPeerForwarderIsReadySender_state",
                          _received_register_peer_complete=self._received_register_peer_complete,
                          _received_acknowledge_register_peer=self._received_acknowledge_register_peer)
        self._logger.debug("try_send")
        should_we_send = self._should_we_send()
        if should_we_send:
            self._finished = True
            self._send_peer_register_forwarder_is_ready_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        is_enabled = self._is_enabled()
        send_independent_of_time = self._send_independent_of_time()
        result = (
                not self._finished
                and (
                        (is_time and is_enabled) or
                        send_independent_of_time
                )
        )
        return result

    def _send_independent_of_time(self):
        received_acknowledge_register_peer = (not self._behavior_config.needs_to_send_register_peer
                                              or self._received_acknowledge_register_peer)
        received_register_peer_complete = (not self._behavior_config.needs_to_send_acknowledge_register_peer
                                           or self._received_register_peer_complete)
        send_independent_of_time = (received_acknowledge_register_peer
                                    and received_register_peer_complete)
        return send_independent_of_time

    def _is_enabled(self):
        received_acknowledge_register_peer = (not self._behavior_config.needs_to_send_register_peer
                                              or self._received_acknowledge_register_peer)
        return received_acknowledge_register_peer

    def _send_peer_register_forwarder_is_ready_to_frontend(self):
        self._logger.debug("send")
        message = PeerRegisterForwarderIsReadyMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def is_ready_to_close(self):
        return self._finished


class RegisterPeerForwarderIsReadySenderFactory:

    def create(self,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               timer: Timer,
               out_control_socket: Socket,
               behavior_config: RegisterPeerForwarderBehaviorConfig):
        return RegisterPeerForwarderIsReadySender(
            peer=peer,
            my_connection_info=my_connection_info,
            behavior_config=behavior_config,
            timer=timer,
            out_control_socket=out_control_socket
        )
