from collections import OrderedDict
from typing import (
    Dict,
    List,
)

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.clock import Clock
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender import (
    PayloadMessageSender,
)
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender_factory import (
    PayloadMessageSenderFactory,
)
from exasol.analytics.udf.communication.peer_communicator.payload_message_sender_timeout_config import (
    PayloadMessageSenderTimeoutConfig,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.serialization import serialize_message
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Socket,
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PayloadSender:
    def __init__(
        self,
        my_connection_info: ConnectionInfo,
        peer: Peer,
        sender: Sender,
        clock: Clock,
        out_control_socket: Socket,
        payload_message_sender_timeout_config: PayloadMessageSenderTimeoutConfig,
        payload_message_sender_factory: PayloadMessageSenderFactory,
    ):
        self._out_control_socket = out_control_socket
        self._payload_message_sender_timeout_config = (
            payload_message_sender_timeout_config
        )
        self._clock = clock
        self._peer = peer
        self._my_connection_info = my_connection_info
        self._payload_message_sender_factory = payload_message_sender_factory
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.model_dump(),
            my_connection_info=self._my_connection_info.model_dump(),
        )
        self._next_send_payload_sequence_number = 0
        self._payload_message_sender_dict: dict[int, PayloadMessageSender] = (
            OrderedDict()
        )

    def try_send(self):
        for payload_sender in self._payload_message_sender_dict.values():
            payload_sender.try_send()

    def received_acknowledge_payload(self, message: messages.AcknowledgePayload):
        self._logger.info("received_acknowledge_payload", message=message.model_dump())
        if message.sequence_number in self._payload_message_sender_dict:
            self._payload_message_sender_dict[message.sequence_number].stop()
            del self._payload_message_sender_dict[message.sequence_number]
            self._out_control_socket.send(
                serialize_message(messages.Message(root=message))
            )

    def send_payload(self, message: messages.Payload, frames: list[Frame]):
        self._logger.info("send_payload", message=message.model_dump())
        self._payload_message_sender_dict[message.sequence_number] = (
            self._payload_message_sender_factory.create(
                message=message,
                frames=frames,
                sender=self._sender,
                out_control_socket=self._out_control_socket,
                clock=self._clock,
                payload_message_sender_timeout_config=self._payload_message_sender_timeout_config,
            )
        )

    def is_ready_to_stop(self):
        return len(self._payload_message_sender_dict) == 0
