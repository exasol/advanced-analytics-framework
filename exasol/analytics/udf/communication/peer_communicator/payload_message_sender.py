from typing import List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import Timer
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Socket,
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PayloadMessageSender:
    def __init__(
        self,
        message: messages.Payload,
        frames: List[Frame],
        retry_timer: Timer,
        abort_timer: Timer,
        sender: Sender,
        out_control_socket: Socket,
    ):
        self._logger = LOGGER.bind(message=message)
        self._abort_timer = abort_timer
        self._out_control_socket = out_control_socket
        self._sender = sender
        self._retry_timer = retry_timer
        self._frames = frames
        self._message = message
        self._finished = False
        self._send_attempt_count = 0
        self._send_payload()

    def _send_payload(self):
        self._send_attempt_count += 1
        if self._send_attempt_count < 2:
            self._logger.debug("send", send_attempt_count=self._send_attempt_count)
        else:
            self._logger.warning("resend", send_attempt_count=self._send_attempt_count)

        self._sender.send_multipart(self._frames)

    def try_send(self):
        should_we_send_abort = self._should_we_send_abort()
        if should_we_send_abort:
            self._send_abort()
            self._finished = True
            return
        should_we_send_payload = self._should_we_send_payload()
        if should_we_send_payload:
            self._send_payload()
            self._retry_timer.reset_timer()

    def stop(self):
        if self._send_attempt_count > 1:
            self._logger.warning("stop payload message sender", message=self._message)
        self._finished = True

    def _should_we_send_abort(self):
        is_time = self._abort_timer.is_time()
        is_enabled = not self._finished
        return is_time and is_enabled

    def _should_we_send_payload(self):
        is_time = self._retry_timer.is_time()
        is_enabled = not self._finished
        return is_time and is_enabled

    def _send_abort(self):
        abort_payload_message = messages.AbortPayload(
            payload=self._message, reason="Send timeout reached"
        )
        serialized_message = serialize_message(abort_payload_message)
        self._out_control_socket.send(serialized_message)
