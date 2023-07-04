import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


import enum


class _State(enum.IntFlag):
    Init = enum.auto()
    Enabled = enum.auto()
    Finished = enum.auto()


class PeerIsReadySender:


    def __init__(self,
                 out_control_socket: Socket,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 timer: Timer):
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._state = _State.Init
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict())

    def enable(self):
        self._logger.debug("enable")
        self._state |= _State.Enabled

    def reset_timer(self):
        self._logger.debug("reset_timer")
        self._timer.reset_timer()

    def send_if_necessary(self, force=False):
        self._logger.debug("send_if_necessary")
        should_we_send = self._should_we_send()
        if should_we_send or force:
            self._state |= _State.Finished
            self._send_peer_is_ready_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and (_State.Finished not in self._state) and (_State.Enabled in self._state)
        return result

    def _send_peer_is_ready_to_frontend(self):
        self._logger.debug("send")
        message = PeerIsReadyToReceiveMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)
