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
    RECEIVED_CLOSE_CONNECTION = auto()
    RECEIVED_ACKKNOWLEDGE_CLOSE_CONNECTION = auto()
    FINISHED = auto()


class ConnectionIsClosedSender:
    def __init__(
        self,
        out_control_socket: Socket,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        timer: Timer,
    ):
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._states = _States.INIT
        self._logger = LOGGER.bind(
            peer=self._peer.model_dump(),
            my_connection_info=my_connection_info.model_dump(),
        )
        self._logger.debug("init")

    def received_close_connection(self):
        self._logger.debug("received_close_connection", states=self._states)
        self._states |= _States.RECEIVED_CLOSE_CONNECTION
        self._timer.reset_timer()

    def received_acknowledge_close_connection(self):
        self._logger.debug("received_acknowledge_close", states=self._states)
        self._states |= _States.RECEIVED_ACKKNOWLEDGE_CLOSE_CONNECTION

    def try_send(self):
        self._logger.debug("try_send", states=self._states)
        should_we_send = self._should_we_send()
        if should_we_send:
            self._states |= _States.FINISHED
            self._send_connection_is_closed_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        send_time_dependent = _States.RECEIVED_CLOSE_CONNECTION in self._states
        send_time_independent = (
            _States.RECEIVED_CLOSE_CONNECTION in self._states
            and _States.RECEIVED_ACKKNOWLEDGE_CLOSE_CONNECTION in self._states
        )
        finished = _States.FINISHED in self._states
        result = not finished and (
            (is_time and send_time_dependent) or send_time_independent
        )
        self._logger.debug(
            "_should_we_send",
            result=result,
            is_time=is_time,
            send_time_dependent=send_time_dependent,
            send_time_independent=send_time_independent,
            states=self._states,
        )
        return result

    def _send_connection_is_closed_to_frontend(self):
        self._logger.debug("send", states=self._states)
        message = messages.ConnectionIsClosed(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)


class ConnectionIsClosedSenderFactory:
    def create(
        self,
        out_control_socket: Socket,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        timer: Timer,
    ) -> ConnectionIsClosedSender:
        peer_is_closed_sender = ConnectionIsClosedSender(
            out_control_socket=out_control_socket,
            timer=timer,
            peer=peer,
            my_connection_info=my_connection_info,
        )
        return peer_is_closed_sender
