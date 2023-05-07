from typing import Dict, List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.messages import PayloadMessage, AcknowledgePayloadMessage, \
    Message
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.sender import Sender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory \
    import Frame, Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PayloadReceiver:
    def __init__(self,
                 peer: Peer,
                 my_connection_info: ConnectionInfo,
                 out_control_socket: Socket,
                 sender: Sender):
        self._peer = peer
        self._my_connection_info = my_connection_info
        self._out_control_socket = out_control_socket
        self._sender = sender
        self._logger = LOGGER.bind(
            peer=self._peer.dict(),
            my_connection_info=self._my_connection_info.dict(),
        )
        self._next_received_payload_sequence_number = 0
        self._received_payload_dict: Dict[int, List[Frame]] = {}

    def received_payload(self, message: PayloadMessage, frames: List[Frame]):
        self._logger.info("received_payload", message=message.dict())
        self._send_acknowledge_payload_message(message.sequence_number)
        if message.sequence_number == self._next_received_payload_sequence_number:
            self._forward_new_message_directly(message, frames)
            self._forward_messages_from_buffer()
        else:
            self._add_new_message_to_buffer(message, frames)

    def _add_new_message_to_buffer(self, message: PayloadMessage, frames: List[Frame]):
        self._logger.info("put_to_buffer", message=message.dict())
        self._received_payload_dict[message.sequence_number] = frames

    def _forward_new_message_directly(self, message: PayloadMessage, frames: List[Frame]):
        self._logger.info("forward_from_message", message=message.dict())
        self._forward_received_payload(frames)

    def _forward_messages_from_buffer(self):
        while self._next_received_payload_sequence_number in self._received_payload_dict:
            self._logger.info("forward_from_buffer",
                              _next_recieved_payload_sequence_number=self._next_received_payload_sequence_number,
                              _received_payload_dict_keys=list(self._received_payload_dict.keys()))
            next_frames = self._received_payload_dict.pop(self._next_received_payload_sequence_number)
            self._forward_received_payload(next_frames)

    def _send_acknowledge_payload_message(self, sequence_number: int):
        acknowledge_payload_message = AcknowledgePayloadMessage(
            source=Peer(connection_info=self._my_connection_info),
            sequence_number=sequence_number
        )
        self._logger.info("_send_acknowledge_payload_message", message=acknowledge_payload_message.dict())
        self._sender.send(message=Message(__root__=acknowledge_payload_message))

    def _forward_received_payload(self, frames: List[Frame]):
        self._out_control_socket.send_multipart(frames)
        self._next_received_payload_sequence_number += 1

    def is_ready_to_close(self) -> bool:
        return len(self._received_payload_dict) == 0
