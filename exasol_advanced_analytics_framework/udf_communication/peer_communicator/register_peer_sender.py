from typing import Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.register_peer_connection import \
    RegisterPeerConnection
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer

LOGGER: FilteringBoundLogger = structlog.get_logger()


class RegisterPeerSender():
    def __init__(self,
                 register_peer_connection: Optional[RegisterPeerConnection],
                 my_connection_info: ConnectionInfo,
                 peer: Peer,
                 timer: Timer):
        self._register_peer_connection = register_peer_connection
        self._my_connection_info = my_connection_info
        self._timer = timer
        self._finished = False
        self._peer = peer
        self._send_attempt_count = 0
        self._logger = LOGGER.bind(
            peer=peer.dict(),
            my_connection_info=my_connection_info.dict())

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
        if self._register_peer_connection is not None:
            self._send_attempt_count += 1
            self._logger.debug("send", send_attempt_count=self._send_attempt_count)
            self._register_peer_connection.forward(self._peer)

    def _should_we_send(self):
        is_time = self._timer.is_time()
        result = is_time and not self._finished
        return result
