import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PeerIsReadySender:
    def __init__(self,
                 out_control_socket: Socket,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 timer: Timer):
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._finished = False
        self._enabled = False
        self._logger = LOGGER.bind(
            module_name=__name__,
            clazz=self.__class__.__name__,
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict())

    def enable(self):
        self._logger.info("enable")
        self._enabled = True

    def reset_timer(self):
        self._logger.info("reset_timer")
        self._timer.reset_timer()

    def send_if_necessary(self, force=False):
        self._logger.info("send_if_necessary")
        should_we_send = self._should_we_send()
        if should_we_send or force:
            self._finished = True
            self._send_peer_is_ready_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished and self._enabled
        return result

    def _send_peer_is_ready_to_frontend(self):
        self._logger.info("send")
        message = PeerIsReadyToReceiveMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)
