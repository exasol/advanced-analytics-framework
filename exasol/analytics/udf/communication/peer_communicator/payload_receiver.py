from typing import (
    Dict,
    List,
)

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    Socket,
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PayloadReceiver:
    def __init__(
        self,
        peer: Peer,
        my_connection_info: ConnectionInfo,
        out_control_socket: Socket,
        sender: Sender,
    ):
        self._peer = peer
        self._my_connection_info = my_connection_info
        self._out_control_socket = out_control_socket
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.model_dump(),
            my_connection_info=self._my_connection_info.model_dump(),
        )
        self._next_received_payload_sequence_number = 0
        self._received_payload_dict: dict[int, list[Frame]] = {}

    def received_payload(self, message: messages.Payload, frames: list[Frame]):
        self._logger.info("received_payload", message=message.model_dump())
        self._send_acknowledge_payload_message(message.sequence_number)
        if message.sequence_number == self._next_received_payload_sequence_number:
            self._forward_new_message_directly(message, frames)
            self._forward_messages_from_buffer()
        elif message.sequence_number > self._next_received_payload_sequence_number:
            self._add_new_message_to_buffer(message, frames)

    def _add_new_message_to_buffer(
        self, message: messages.Payload, frames: list[Frame]
    ):
        self._logger.info("put_to_buffer", message=message.model_dump())
        self._received_payload_dict[message.sequence_number] = frames

    def _forward_new_message_directly(
        self, message: messages.Payload, frames: list[Frame]
    ):
        self._logger.info("forward_from_message", message=message.model_dump())
        self._forward_received_payload(frames)

    def _forward_messages_from_buffer(self):
        while (
            self._next_received_payload_sequence_number in self._received_payload_dict
        ):
            self._logger.info(
                "forward_from_buffer",
                _next_recieved_payload_sequence_number=self._next_received_payload_sequence_number,
                _received_payload_dict_keys=list(self._received_payload_dict.keys()),
            )
            next_frames = self._received_payload_dict.pop(
                self._next_received_payload_sequence_number
            )
            self._forward_received_payload(next_frames)

    def _send_acknowledge_payload_message(self, sequence_number: int):
        acknowledge_payload_message = messages.AcknowledgePayload(
            source=Peer(connection_info=self._my_connection_info),
            sequence_number=sequence_number,
            destination=self._peer,
        )
        self._logger.info(
            "_send_acknowledge_payload_message",
            message=acknowledge_payload_message.model_dump(),
        )
        self._sender.send(message=messages.Message(root=acknowledge_payload_message))

    def _forward_received_payload(self, frames: list[Frame]):
        self._out_control_socket.send_multipart(frames)
        self._next_received_payload_sequence_number += 1

    def is_ready_to_stop(self) -> bool:
        is_ready = len(self._received_payload_dict) == 0
        self._logger.debug("payload_receiver_is_ready", is_ready=is_ready)
        return is_ready
