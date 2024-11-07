from exasol.analytics.udf.communication.peer_communicator.payload_handler import PayloadHandler
from exasol.analytics.udf.communication.peer_communicator.payload_receiver import PayloadReceiver
from exasol.analytics.udf.communication.peer_communicator.payload_sender import PayloadSender


class PayloadHandlerFactory:
    def create(self,
               payload_sender: PayloadSender,
               payload_receiver: PayloadReceiver) -> PayloadHandler:
        return PayloadHandler(
            payload_sender=payload_sender,
            payload_receiver=payload_receiver
        )
