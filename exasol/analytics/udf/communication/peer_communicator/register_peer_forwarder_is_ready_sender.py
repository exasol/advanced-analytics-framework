from dataclasses import asdict
from enum import IntFlag, auto

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder_behavior_config \
    import RegisterPeerForwarderBehaviorConfig
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class _States(IntFlag):
    INIT = auto()
    REGISTER_PEER_ACKNOWLEDGED = auto()
    REGISTER_PEER_COMPLETED = auto()
    FINISHED = auto()


class RegisterPeerForwarderIsReadySender:

    def __init__(self,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 timer: Timer,
                 out_control_socket: Socket,
                 behavior_config: RegisterPeerForwarderBehaviorConfig):
        self._behavior_config = behavior_config
        self._peer = peer
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._states = _States.INIT
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict(),
            behavior_config=asdict(self._behavior_config))

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._received_acknowledge_register_peer = True
        self._timer.reset_timer()
        self._states |= _States.REGISTER_PEER_ACKNOWLEDGED
        self._logger.debug("received_register_peer_complete", states=self._states)

    def received_register_peer_complete(self):
        self._states |= _States.REGISTER_PEER_COMPLETED
        self._logger.debug("received_register_peer_complete", states=self._states)

    def try_send(self):
        self._logger.debug("try_send", states=self._states)
        should_we_send = self._should_we_send()
        if should_we_send:
            self._states |= _States.FINISHED
            self._send_peer_register_forwarder_is_ready_to_frontend()

    def _should_we_send(self) -> bool:
        is_time = self._timer.is_time()
        send_time_dependent = self._is_send_time_dependent()
        send_time_independent = self._is_send_time_independent()
        result = (
                not _States.FINISHED in self._states
                and (
                        (is_time and send_time_dependent) or
                        send_time_independent
                )
        )
        self._logger.debug("_should_we_send",
                           result=result,
                           is_time=is_time,
                           send_time_dependent=send_time_dependent,
                           send_time_independent=send_time_independent,
                           states=self._states)
        return result

    def _is_send_time_independent(self):
        received_acknowledge_register_peer = (
                not self._behavior_config.needs_to_send_register_peer
                or _States.REGISTER_PEER_ACKNOWLEDGED in self._states
        )
        received_register_peer_complete = (
                not self._behavior_config.needs_to_send_acknowledge_register_peer
                or _States.REGISTER_PEER_COMPLETED in self._states
        )
        send_independent_of_time = (
                received_acknowledge_register_peer
                and received_register_peer_complete
        )
        return send_independent_of_time

    def _is_send_time_dependent(self):
        received_acknowledge_register_peer = (
                not self._behavior_config.needs_to_send_register_peer
                or _States.REGISTER_PEER_ACKNOWLEDGED in self._states
        )
        return received_acknowledge_register_peer

    def _send_peer_register_forwarder_is_ready_to_frontend(self):
        self._logger.debug("send")
        message = messages.PeerRegisterForwarderIsReady(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def is_ready_to_stop(self) -> bool:
        return _States.FINISHED in self._states


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
