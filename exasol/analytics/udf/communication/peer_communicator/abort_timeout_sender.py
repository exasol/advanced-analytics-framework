from enum import IntFlag, auto

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class _States(IntFlag):
    INIT = auto()
    FINISHED = auto()


class AbortTimeoutSender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 reason: str,
                 out_control_socket: Socket,
                 timer: Timer):
        self._reason = reason
        self._timer = timer
        self._out_control_socket = out_control_socket
        self._states = _States.INIT
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

    def stop(self):
        self._logger.info("stop")
        self._states |= _States.FINISHED

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
        result = is_time and not _States.FINISHED in self._states
        return result

    def _send_timeout_to_frontend(self):
        self._logger.debug("send")
        message = messages.Timeout(reason=self._reason)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)


class AbortTimeoutSenderFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               reason: str,
               out_control_socket: Socket,
               timer: Timer) -> AbortTimeoutSender:
        abort_timeout_sender = AbortTimeoutSender(
            out_control_socket=out_control_socket,
            timer=timer,
            my_connection_info=my_connection_info,
            peer=peer,
            reason=reason
        )
        return abort_timeout_sender
