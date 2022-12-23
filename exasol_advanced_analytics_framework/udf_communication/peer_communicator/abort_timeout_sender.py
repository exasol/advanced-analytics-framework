import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import TimeoutMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class AbortTimeoutSender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 out_control_socket: Socket,
                 timer: Timer):
        self._timer = timer
        self._out_control_socket = out_control_socket
        self._finished = False
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

    def reset_timer(self):
        self._logger.info("reset_timer")
        self._timer.reset_timer()

    def stop(self):
        self._logger.info("stop")
        self._finished = True

    def send_if_necessary(self):
        self._logger.debug("send_if_necessary")
        should_we_send = self._should_we_send()
        if should_we_send:
            self._finished = True
            self._send_timeout_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished
        return result

    def _send_timeout_to_frontend(self):
        self._logger.debug("send")
        message = TimeoutMessage()
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)
