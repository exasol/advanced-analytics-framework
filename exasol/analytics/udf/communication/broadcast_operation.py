from typing import Optional

import structlog
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication import messages
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.serialization import (
    deserialize_message,
    serialize_message,
)
from exasol.analytics.udf.communication.socket_factory.abstract import (
    Frame,
    SocketFactory,
)
from exasol.analytics.utils.errors import UninitializedAttributeError

_LOGGER: FilteringBoundLogger = structlog.getLogger()

LOCALHOST_LEADER_RANK = 0
MULTI_NODE_LEADER_RANK = 0


class BroadcastOperation:

    def __init__(
        self,
        sequence_number: int,
        value: Optional[bytes],
        localhost_communicator: PeerCommunicator,
        multi_node_communicator: Optional[PeerCommunicator],
        socket_factory: SocketFactory,
    ):
        self._socket_factory = socket_factory
        self._value = value
        self._sequence_number = sequence_number
        self._multi_node_communicator = multi_node_communicator
        self._localhost_communicator = localhost_communicator
        self._logger = _LOGGER.bind(
            sequence_number=self._sequence_number,
        )

    def __call__(self) -> bytes:
        if self._localhost_communicator.rank > LOCALHOST_LEADER_RANK:
            return self._receive_from_localhost_leader()
        return self._send_messages_to_local_peers()

    def _receive_from_localhost_leader(self) -> bytes:
        self._logger.info("_receive_from_localhost_leader")
        leader = self._localhost_communicator.leader
        frames = self._localhost_communicator.recv(peer=leader)
        message = deserialize_message(frames[0].to_bytes(), messages.Message)
        specific_message_obj = self._get_and_check_specific_message_obj(message)
        self._check_sequence_number(specific_message_obj=specific_message_obj)
        return frames[1].to_bytes()

    def _send_messages_to_local_peers(self) -> bytes:
        if self._multi_node_communicator is None:
            raise UninitializedAttributeError("Multi node communicator is undefined.")
        if self._multi_node_communicator.rank > 0:
            return self._forward_from_multi_node_leader()
        return self._send_messages_from_multi_node_leaders()

    def _forward_from_multi_node_leader(self) -> bytes:
        self._logger.info("_forward_from_multi_node_leader")
        value_frame = self.receive_value_frame_from_multi_node_leader()
        leader = self._localhost_communicator.leader
        peers = [
            peer for peer in self._localhost_communicator.peers() if peer != leader
        ]

        for peer in peers:
            frames = self._construct_broadcast_message(
                destination=peer, leader=leader, value_frame=value_frame
            )
            self._localhost_communicator.send(peer=peer, message=frames)

        return value_frame.to_bytes()

    def receive_value_frame_from_multi_node_leader(self) -> Frame:
        if self._multi_node_communicator is None:
            raise UninitializedAttributeError("Multi node communicator is undefined.")
        leader = self._multi_node_communicator.leader
        frames = self._multi_node_communicator.recv(leader)
        self._logger.info("received")
        message = deserialize_message(frames[0].to_bytes(), messages.Message)
        specific_message_obj = self._get_and_check_specific_message_obj(message)
        self._check_sequence_number(specific_message_obj=specific_message_obj)
        return frames[1]

    def _send_messages_from_multi_node_leaders(self) -> bytes:
        self._send_messages_to_local_leaders()
        self._send_messages_to_local_peers_from_multi_node_leaders()
        if self._value is None:
            raise UninitializedAttributeError("Value is unset.")
        return self._value

    def _send_messages_to_local_leaders(self):
        if self._multi_node_communicator is None:
            return

        self._logger.info("_send_messages_to_local_leaders")
        leader = self._multi_node_communicator.leader
        peers = [
            peer for peer in self._multi_node_communicator.peers() if peer != leader
        ]

        for peer in peers:
            value_frame = self._socket_factory.create_frame(self._value)
            frames = self._construct_broadcast_message(
                destination=peer, leader=leader, value_frame=value_frame
            )
            self._multi_node_communicator.send(peer=peer, message=frames)

    def _send_messages_to_local_peers_from_multi_node_leaders(self):
        self._logger.info("_send_messages_to_local_peers_from_multi_node_leaders")
        leader = self._localhost_communicator.leader
        peers = [p for p in self._localhost_communicator.peers() if p != leader]
        for peer in peers:
            value_frame = self._socket_factory.create_frame(self._value)
            frames = self._construct_broadcast_message(
                destination=peer, leader=leader, value_frame=value_frame
            )
            self._localhost_communicator.send(peer=peer, message=frames)

    def _check_sequence_number(self, specific_message_obj: messages.Broadcast):
        if specific_message_obj.sequence_number != self._sequence_number:
            raise RuntimeError(
                f"Got message with different sequence number. "
                f"We expect the sequence number {self._sequence_number} "
                f"but we got {self._sequence_number} in message {specific_message_obj}"
            )

    def _get_and_check_specific_message_obj(
        self, message: messages.Message
    ) -> messages.Broadcast:
        specific_message_obj = message.root
        if not isinstance(specific_message_obj, messages.Broadcast):
            raise TypeError(
                f"Received the wrong message type. "
                f"Expected {messages.Broadcast.__name__} got {type(message)}. "
                f"For message {message}."
            )
        return specific_message_obj

    def _construct_broadcast_message(
        self, destination: Peer, leader: Peer, value_frame: Frame
    ):
        message = messages.Broadcast(
            sequence_number=self._sequence_number,
            destination=destination,
            source=leader,
        )
        serialized_message = serialize_message(message)
        frames = [self._socket_factory.create_frame(serialized_message), value_frame]
        return frames
