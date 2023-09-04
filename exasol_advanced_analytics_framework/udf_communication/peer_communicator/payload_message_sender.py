from typing import List

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.timer import Timer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import Frame, Socket


class PayloadMessageSender:
    def __init__(self, message: messages.Payload,
                 frames: List[Frame],
                 retry_timer: Timer,
                 abort_timer: Timer,
                 sender: Sender,
                 out_control_socket: Socket):
        self._abort_timer = abort_timer
        self._out_control_socket = out_control_socket
        self._sender = sender
        self._retry_timer = retry_timer
        self._frames = frames
        self._message = message
        self._finished = False
        self._send_payload()

    def _send_payload(self):
        self._sender.send_multipart(self._frames)

    def try_send(self):
        should_we_abort = self._should_we_abort()
        if should_we_send_abort:
            self._send_abort()
            self._finished = True
            return
        should_we_send_payload = self._should_we_send_payload()
        if should_we_send_payload:
            self._send_payload()
            self._retry_timer.reset_timer()

    def stop(self):
        self._finished = True

    def _should_we_abort(self):
        is_time = self._abort_timer.is_time()
        is_enabled = not self._finished
        return is_time and is_enabled

    def _should_we_send_payload(self):
        is_time = self._retry_timer.is_time()
        is_enabled = not self._finished
        return is_time and is_enabled

    def _send_abort(self):
        abort_payload_message = messages.AbortPayload(
            payload=self._message,
            reason="Send timeout reached"
        )
        serialized_message = serialize_message(abort_payload_message)
        self._out_control_socket.send(serialized_message)
