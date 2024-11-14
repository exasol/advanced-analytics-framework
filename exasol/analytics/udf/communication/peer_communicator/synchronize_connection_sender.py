import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import Timer

LOGGER: FilteringBoundLogger = structlog.get_logger()


class SynchronizeConnectionSender:
    def __init__(self,
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 sender: Sender,
                 timer: Timer):
        self._my_connection_info = my_connection_info
        self._peer = peer
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
        if self._send_attempt_count < 2:
            self._logger.debug("send", send_attempt_count=self._send_attempt_count)
        else:
            self._logger.warning("resend", send_attempt_count=self._send_attempt_count)
        message = messages.Message(
            __root__=messages.SynchronizeConnection(
                source=self._my_connection_info,
                destination=self._peer,
                attempt=self._send_attempt_count
            ))
        self._sender.send(message)

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished
        return result


class SynchronizeConnectionSenderFactory:
    def create(self,
               my_connection_info: ConnectionInfo,
               peer: Peer,
               sender: Sender,
               timer: Timer) -> SynchronizeConnectionSender:
        synchronize_connection_sender = SynchronizeConnectionSender(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            timer=timer
        )
        return synchronize_connection_sender
