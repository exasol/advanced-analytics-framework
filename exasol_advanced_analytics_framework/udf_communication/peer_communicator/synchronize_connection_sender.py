import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import Message, SynchronizeConnectionMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer

LOGGER: FilteringBoundLogger = structlog.get_logger()


class SynchronizeConnectionSender():
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 sender: Sender,
                 timer: Timer):
        self._my_connection_info = my_connection_info
        self._timer = timer
        self._sender = sender
        self._finished = False
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

    def stop(self):
        self._logger.debug("stop")
        self._finished = True

    def send_if_necessary(self, force=False):
        self._logger.debug("send_if_necessary")
        should_we_send = self._should_we_send()
        if should_we_send or force:
            self._send()
            self._timer.reset_timer()

    def _send(self):
        self._logger.debug("send")
        message = Message(__root__=SynchronizeConnectionMessage(source=self._my_connection_info))
        self._sender.send(message)

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished
        return result
