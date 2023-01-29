import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import TimeoutMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


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
        self._received_synchronize_connection = False
        self._received_acknowledge_connection = False
        self._received_acknowledge_register_peer = False
        self._finished = False
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

    def reset_timer(self):
        self._logger.info("reset_timer")
        self._timer.reset_timer()

    def try_send(self):
        self._logger.debug("try_send")
        should_we_send = self._should_we_send()
        if should_we_send:
            self._finished = True
            self._send_timeout_to_frontend()

    def _should_we_send(self):
        is_time = self._timer.is_time()
        abort_stopped = self._abort_stopped()
        result = is_time and not self._finished and not abort_stopped
        return result

    def _abort_stopped(self):
        connection_ok = self._received_synchronize_connection or self._received_acknowledge_connection
        received_acknowledge_register_peer = self._needs_acknowledge_register_peer \
                                             or self._received_acknowledge_register_peer
        abort_stopped = connection_ok and received_acknowledge_register_peer
        return abort_stopped

    def _send_timeout_to_frontend(self):
        self._logger.debug("send")
        message = TimeoutMessage()
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)

    def received_synchronize_connection(self):
        self._logger.info("received_synchronize_connection")
        self._received_synchronize_connection = True

    def received_acknowledge_connection(self):
        self._logger.info("received_acknowledge_connection")
        self._received_acknowledge_connection = True

    def received_acknowledge_register_peer(self):
        self._logger.info("received_acknowledge_register_peer")
        self._received_acknowledge_register_peer = True


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
