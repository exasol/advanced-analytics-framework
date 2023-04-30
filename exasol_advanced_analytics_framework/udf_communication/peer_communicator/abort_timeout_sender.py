from enum import IntFlag, auto

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class _States(IntFlag):
    INIT = auto()
    CONNECTION_SYNCHRONIZED = auto()
    CONNECTION_ACKNOWLEDGED = auto()
    REGISTER_PEER_ACKNOWLEDGED = auto()
    FINISHED = auto()


class AbortTimeoutSender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 out_control_socket: Socket,
                 timer: Timer,
                 needs_acknowledge_register_peer: bool):
        self._needs_acknowledge_register_peer = needs_acknowledge_register_peer
        self._timer = timer
        self._out_control_socket = out_control_socket
        self._states = _States.INIT
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

    def reset_timer(self):
        self._logger.info("reset_timer", states=self._states)
        self._timer.reset_timer()

    def try_send(self):
        self._logger.debug("try_send", states=self._states)
        should_we_send = self._should_we_send()
        if should_we_send:
            self._states |= _States.FINISHED
            self._send_timeout_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        abort_stopped = self._abort_stopped()
        result = is_time and not _States.FINISHED in self._states and not abort_stopped
        return result

    def _abort_stopped(self):
        connection_ok = (
                _States.CONNECTION_SYNCHRONIZED in self._states
                or _States.CONNECTION_ACKNOWLEDGED in self._states
        )
        received_acknowledge_register_peer = (
                not self._needs_acknowledge_register_peer
                or _States.REGISTER_PEER_ACKNOWLEDGED in self._states
        )
        abort_stopped = connection_ok and received_acknowledge_register_peer
        return abort_stopped

    def _send_timeout_to_frontend(self):
        self._logger.debug("send", states=self._states)
        message = messages.Timeout(reason="Establishing connection aborted after timeout.")
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def received_synchronize_connection(self):
        self._states |= _States.CONNECTION_SYNCHRONIZED
        self._logger.debug("received_synchronize_connection", states=self._states)

    def received_acknowledge_connection(self):
        self._states |= _States.CONNECTION_ACKNOWLEDGED
        self._logger.debug("received_acknowledge_connection", states=self._states)

    def received_acknowledge_register_peer(self):
        self._states |= _States.REGISTER_PEER_ACKNOWLEDGED
        self._logger.debug("received_acknowledge_register_peer", states=self._states)


class AbortTimeoutSenderFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               out_control_socket: Socket,
               timer: Timer,
               needs_acknowledge_register_peer: bool) -> AbortTimeoutSender:
        abort_timeout_sender = AbortTimeoutSender(
            out_control_socket=out_control_socket,
            timer=timer,
            my_connection_info=my_connection_info,
            peer=peer,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer
        )
        return abort_timeout_sender
