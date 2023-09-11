import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer

LOGGER: FilteringBoundLogger = structlog.get_logger()


class CloseConnectionSender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 sender: Sender,
                 timer: Timer):
        self._my_connection_info = my_connection_info
        self._timer = timer
        self._sender = sender
        self._finished = False
        self._send_attempt_count = 0
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())
        self._logger.debug("init")

    def stop(self):
        self._logger.debug("stop")
        self._finished = True

    def try_send(self, force=False):
        self._logger.debug("try_send")
        should_we_send = self._should_we_send()
        if should_we_send or force:
            self._send()
            self._timer.reset_timer()

    def _send(self):
        self._send_attempt_count += 1
        self._logger.debug("send", send_attempt_count=self._send_attempt_count)
        message = messages.Message(__root__=messages.CloseConnection(source=self._my_connection_info))
        self._sender.send(message)

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished
        return result


class CloseConnectionSenderFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               sender: Sender,
               timer: Timer) -> CloseConnectionSender:
        close_connection_sender = CloseConnectionSender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=timer
        )
        return close_connection_sender