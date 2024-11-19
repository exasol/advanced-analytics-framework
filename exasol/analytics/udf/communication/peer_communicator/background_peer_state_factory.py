from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.background_peer_state import (
    BackgroundPeerState,
)
from exasol.analytics.udf.communication.peer_communicator.background_thread.connection_closer.connection_closer import (
    ConnectionCloser,
)
from exasol.analytics.udf.communication.peer_communicator.connection_establisher import (
    ConnectionEstablisher,
)
from exasol.analytics.udf.communication.peer_communicator.payload_handler import (
    PayloadHandler,
)
from exasol.analytics.udf.communication.peer_communicator.register_peer_forwarder import (
    RegisterPeerForwarder,
)
from exasol.analytics.udf.communication.peer_communicator.sender import Sender


class BackgroundPeerStateFactory:

    def create(
        self,
        my_connection_info: ConnectionInfo,
        peer: Peer,
        sender: Sender,
        connection_establisher: ConnectionEstablisher,
        connection_closer: ConnectionCloser,
        register_peer_forwarder: RegisterPeerForwarder,
        payload_handler: PayloadHandler,
    ) -> BackgroundPeerState:
        return BackgroundPeerState(
            my_connection_info=my_connection_info,
            peer=peer,
            sender=sender,
            connection_establisher=connection_establisher,
            connection_closer=connection_closer,
            register_peer_forwarder=register_peer_forwarder,
            payload_handler=payload_handler,
        )
