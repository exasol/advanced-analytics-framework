from typing import List

from exasol_advanced_analytics_framework.udf_communication.messages import PayloadMessage, AcknowledgePayloadMessage
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_receiver import PayloadReceiver
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender import PayloadSender
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract_socket_factory import Frame


class PayloadHandler:
    def __init__(self,
                 payload_sender: PayloadSender,
                 payload_receiver: PayloadReceiver):
        self._payload_receiver = payload_receiver
        self._payload_sender = payload_sender

    def send_payload(self, message: PayloadMessage, frames: List[Frame]):
        self._payload_sender.send_payload(message, frames)

    def received_acknowledge_payload(self, message: AcknowledgePayloadMessage):
        self._payload_sender.received_acknowledge_payload(message)

    def received_payload(self, message: PayloadMessage, frames: List[Frame]):
        self._payload_receiver.received_payload(message, frames)

    def try_send(self):
        self._payload_sender.try_send()

    def is_ready_to_close(self):
        sender_is_ready_to_close = self._payload_sender.is_ready_to_close()
        receiver_is_ready_to_close = self._payload_receiver.is_ready_to_close()
        return sender_is_ready_to_close and receiver_is_ready_to_close
