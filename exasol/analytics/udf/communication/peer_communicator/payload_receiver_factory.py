import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.payload_receiver import PayloadReceiver
from exasol.analytics.udf.communication.peer_communicator.sender import Sender
from exasol.analytics.udf.communication.socket_factory.abstract \
    import Socket

LOGGER: FilteringBoundLogger = structlog.get_logger()


class PayloadReceiverFactory:
    def create(
            self,
            peer: Peer,
            my_connection_info: ConnectionInfo,
            sender: Sender,
            out_control_socket: Socket,
    ) -> PayloadReceiver:
        return PayloadReceiver(
            peer=peer,
            my_connection_info=my_connection_info,
            sender=sender,
            out_control_socket=out_control_socket
        )
