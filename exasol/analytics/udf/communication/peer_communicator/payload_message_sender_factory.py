from typing import List

from exasol.analytics.udf.communication.messages import Payload
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender import \
    PayloadMessageSender
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender_timeout_config \
    import PayloadMessageSenderTimeoutConfig
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.peer_communicator.timer import TimerFactory
from exasol.analytics.udf.communication.socket_factory.abstract import Socket, Frame


class PayloadMessageSenderFactory:
    def __init__(self, timer_factory: TimerFactory):
        self._timer_factory = timer_factory

    def create(self,
               clock: Clock,
               sender: Sender,
               message: Payload,
               frames: List[Frame],
               payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig,
               out_control_socket: Socket) -> PayloadMessageSender:
        retry_timer = self._timer_factory.create(
            clock, payload_message_sender_timeout_config.retry_timeout_in_ms)
        abort_timer = self._timer_factory.create(
            clock, payload_message_sender_timeout_config.abort_timeout_in_ms)
        return PayloadMessageSender(message=message,
                                    frames=frames,
                                    retry_timer=retry_timer,
                                    abort_timer=abort_timer,
                                    sender=sender,
                                    out_control_socket=out_control_socket)
