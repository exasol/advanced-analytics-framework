import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import ConnectionIsReadyMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class ConnectionIsReadySender:
    def __init__(self,
                 out_control_socket: Socket,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 timer: Timer):
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._finished = False
        self._received_synchronize_connection = False
        self._received_acknowledge_connection = False
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict(),
        )
        self._logger.debug("init")

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._received_synchronize_connection = True
        self._timer.reset_timer()

    def received_acknowledge_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._received_acknowledge_connection = True

    def try_send(self):
        self._logger.debug("try_send")
        should_we_send = self._should_we_send()
        if should_we_send:
            self._finished = True
            self._send_connection_is_ready_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        send_time_dependent = self._received_synchronize_connection
        send_time_independent = self._received_acknowledge_connection
        result = (
                not self._finished
                and (
                        (is_time and send_time_dependent) or
                        send_time_independent
                )
        )
        return result

    def _send_connection_is_ready_to_frontend(self):
        self._logger.debug("send")
        message = ConnectionIsReadyMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def is_ready_to_close(self):
        return self._finished


class ConnectionIsReadySenderFactory:
    def create(self,
               out_control_socket: Socket,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               timer: Timer) -> ConnectionIsReadySender:
        peer_is_ready_sender = ConnectionIsReadySender(
            out_control_socket=out_control_socket,
            timer=timer,
            peer=peer,
            my_connection_info=my_connection_info,
        )
        return peer_is_ready_sender
