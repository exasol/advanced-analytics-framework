from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_handler import PayloadHandler
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_receiver import PayloadReceiver
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.payload_sender import PayloadSender


class PayloadHandlerFactory:
    def create(self,
               payload_sender: PayloadSender,
               payload_receiver: PayloadReceiver) -> PayloadHandler:
        return PayloadHandler(
            payload_sender=payload_sender,
            payload_receiver=payload_receiver
        )
