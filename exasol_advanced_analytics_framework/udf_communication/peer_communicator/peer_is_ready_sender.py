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
                 timer: Timer,
                 needs_acknowledge_register_peer: bool,
                 needs_register_peer_complete: bool):
        self._needs_acknowledge_register_peer = needs_acknowledge_register_peer
        self._needs_register_peer_complete = needs_register_peer_complete
        self._timer = timer
        self._peer = peer
        self._out_control_socket = out_control_socket
        self._finished = False
        self._received_synchronize_connection = False
        self._received_acknowledge_connection = False
        self._received_acknowledge_register_peer = False
        self._received_register_peer_complete = False
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=my_connection_info.dict(),
            needs_acknowledge_register_peer=self._needs_acknowledge_register_peer,
            needs_register_peer_complete=self._needs_register_peer_complete
        )
        self._logger.debug("init")

    def received_synchronize_connection(self):
        self._logger.debug("received_synchronize_connection")
        self._received_synchronize_connection = True

    def received_acknowledge_register_peer(self):
        self._logger.debug("received_acknowledge_register_peer")
        self._received_acknowledge_register_peer = True

    def received_acknowledge_connection(self):
        self._logger.debug("received_acknowledge_connection")
        self._received_acknowledge_connection = True

    def received_register_peer_complete(self):
        self._logger.debug("received_register_peer_complete")
        self._received_register_peer_complete = True

    def reset_timer(self):
        self._logger.debug("reset_timer")
        self._timer.reset_timer()

    def try_send(self):
        self._logger.debug("try_send")
        should_we_send = self._should_we_send()
        if should_we_send:
            self._finished = True
            self._send_peer_is_ready_to_frontend()

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
        self._logger.debug("_should_we_send",
                           result=result,
                           is_time=is_time,
                           is_enabled=is_enabled,
                           send_independent_of_time=send_independent_of_time,
                           needs_acknowledge_register_peer=self._needs_acknowledge_register_peer,
                           needs_register_peer_complete=self._needs_register_peer_complete,
                           received_acknowledge_register_peer=self._received_acknowledge_register_peer,
                           received_register_peer_complete=self._received_register_peer_complete,
                           received_synchronize_connection=self._received_synchronize_connection,
                           received_acknowledge_connection=self._received_acknowledge_connection)
        return result

    def _send_independent_of_time(self):
        received_acknowledge_register_peer = (not self._needs_acknowledge_register_peer
                                              or self._received_acknowledge_register_peer)
        received_register_peer_complete = (not self._needs_register_peer_complete
                                           or self._received_register_peer_complete)
        send_independent_of_time = (self._received_acknowledge_connection
                                    and received_acknowledge_register_peer
                                    and received_register_peer_complete)
        return send_independent_of_time

    def _is_enabled(self):
        received_acknowledge_register_peer = (not self._needs_acknowledge_register_peer
                                              or self._received_acknowledge_register_peer)
        is_enabled = (self._received_synchronize_connection
                      and received_acknowledge_register_peer)
        return is_enabled

    def _send_peer_is_ready_to_frontend(self):
        self._logger.debug("send")
        message = PeerIsReadyToReceiveMessage(peer=self._peer)
        serialized_message = serialize_message(message)
        self._out_control_socket.send(serialized_message)


class PeerIsReadySenderFactory:
    def create(self,
               out_control_socket: Socket,
               peer: Peer,
               my_connection_info: ConnectionInfo,
               timer: Timer,
               needs_acknowledge_register_peer: bool,
               needs_register_peer_complete: bool) -> PeerIsReadySender:
        peer_is_ready_sender = PeerIsReadySender(
            out_control_socket=out_control_socket,
            timer=timer,
            peer=peer,
            my_connection_info=my_connection_info,
            needs_acknowledge_register_peer=needs_acknowledge_register_peer,
            needs_register_peer_complete=needs_register_peer_complete,
        )
        return peer_is_ready_sender
