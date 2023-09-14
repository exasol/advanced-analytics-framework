from typing import Optional, List, Dict

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.messages import Gather
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, Frame

LOGGER: FilteringBoundLogger = structlog.getLogger()

LOCALHOST_LEADER_RANK = 0
MULTI_NODE__LEADER_RANK = 0

class GatherOperation:

    def __init__(self,
                 sequence_number: int,
                 value: bytes,
                 localhost_communicator: PeerCommunicator,
                 multi_node_communicator: PeerCommunicator,
                 socket_factory: SocketFactory,
                 number_of_instances_per_node: int):
        self._number_of_instances_per_node = number_of_instances_per_node
        self._socket_factory = socket_factory
        self._value = value
        self._sequence_number = sequence_number
        self._multi_node_communicator = multi_node_communicator
        self._localhost_communicator = localhost_communicator
        self._logger = LOGGER.bind(
            sequence_number=self._sequence_number,
        )

    def __call__(self) -> Optional[List[bytes]]:
        if self._localhost_communicator.rank > LOCALHOST_LEADER_RANK:
            return self._send_to_localhost_leader()
        return self._handle_messages_from_local_peers()

    def _send_to_localhost_leader(self) -> None:
        leader = self._localhost_communicator.leader
        position = self._localhost_communicator.rank
        source = self._localhost_communicator.peer
        value_frame = self._socket_factory.create_frame(self._value)
        frames = self._construct_gather_message(source=source, leader=leader,
                                                position=position, value_frame=value_frame)
        self._logger.info("_send_to_localhost_leader", frame=frames[0].to_bytes())
        self._localhost_communicator.send(peer=leader, message=frames)

    def _handle_messages_from_local_peers(self) -> Optional[List[bytes]]:
        if self._multi_node_communicator.rank > 0:
            return self._forward_to_multi_node_leader()
        return self._handle_messages_from_all_nodes()

    def _forward_to_multi_node_leader(self) -> None:
        self._send_local_leader_message_to_multi_node_leader()
        peers_without_message = set(self._localhost_communicator.peers())
        peers_without_message.remove(self._localhost_communicator.peer)
        while len(peers_without_message) > 0:
            peers_with_messages = self._localhost_communicator.poll_peers()
            for peer in peers_with_messages:
                self._forward_message_for_peer(peer)
                peers_without_message.remove(peer)

    def _forward_message_for_peer(self, peer: Peer):
        frames = self._localhost_communicator.recv(peer)
        message = deserialize_message(frames[0].to_bytes(), messages.Message)
        specific_message_obj = self._get_and_check_specific_message_obj(message)
        self._check_sequence_number(specific_message_obj)
        local_position = self._get_and_check_local_position(specific_message_obj)
        self._logger.info("_forward_message_for_peer", local_position=local_position, peer=peer)
        self._send_to_multi_node_leader(local_position=local_position,
                                        value_frame=frames[1])

    def _send_local_leader_message_to_multi_node_leader(self):
        local_position = LOCALHOST_LEADER_RANK
        value_frame = self._socket_factory.create_frame(self._value)
        self._send_to_multi_node_leader(local_position=local_position, value_frame=value_frame)

    def _send_to_multi_node_leader(self, local_position: int, value_frame: Frame):
        leader = self._multi_node_communicator.leader
        source = self._multi_node_communicator.peer
        base_position = self._multi_node_communicator.rank * self._number_of_instances_per_node
        position = base_position + local_position
        frames = self._construct_gather_message(source=source, leader=leader, position=position,
                                                value_frame=value_frame)
        self._logger.info("_send_to_multi_node_leader", frame=frames[0].to_bytes())
        self._multi_node_communicator.send(peer=leader, message=frames)

    def _construct_gather_message(self, source: Peer, leader: Peer, position: int, value_frame: Frame):
        message = Gather(sequence_number=self._sequence_number,
                         destination=leader,
                         source=source,
                         position=position)
        serialized_message = serialize_message(message)
        frames = [
            self._socket_factory.create_frame(serialized_message),
            value_frame
        ]
        return frames

    def _handle_messages_from_all_nodes(self) -> List[bytes]:
        number_of_instances_in_cluster = self._multi_node_communicator.number_of_peers \
                                         * self._number_of_instances_per_node
        result: Dict[int, bytes] = {MULTI_NODE__LEADER_RANK: self._value}
        localhost_messages_are_done = False
        multi_node_messages_are_done = False
        while not self._is_result_complete(result, number_of_instances_in_cluster):
            if not localhost_messages_are_done:
                localhost_messages_are_done = self._receive_localhost_messages(result)
            if not multi_node_messages_are_done:
                multi_node_messages_are_done = self._receive_multi_node_messages(result, number_of_instances_in_cluster)
        sorted_items = sorted(result.items(), key=lambda kv: kv[0])
        return [v for k, v in sorted_items]

    def _receive_localhost_messages(self, result: Dict[int, bytes]) -> bool:
        if self._number_of_instances_per_node == 1:
            return True
        peers_with_messages = self._localhost_communicator.poll_peers()
        for peer in peers_with_messages:
            frames = self._localhost_communicator.recv(peer)
            self._logger.info("_receive_localhost_messages", frame=frames[0].to_bytes())
            message = deserialize_message(frames[0].to_bytes(), messages.Message)
            specific_message_obj = self._get_and_check_specific_message_obj(message)
            self._check_sequence_number(specific_message_obj)
            local_position = self._get_and_check_local_position(specific_message_obj)
            self._check_if_position_is_already_set(local_position, result, specific_message_obj)
            result[local_position] = frames[1].to_bytes()
        positions_required_by_localhost = range(self._number_of_instances_per_node)
        is_done = set(positions_required_by_localhost).issubset(result.keys())
        return is_done

    def _receive_multi_node_messages(self, result: Dict[int, bytes], number_of_instances_in_cluster: int) -> bool:
        if self._multi_node_communicator.number_of_peers == 1:
            return True
        peers_with_messages = self._multi_node_communicator.poll_peers()
        for peer in peers_with_messages:
            frames = self._multi_node_communicator.recv(peer)
            self._logger.info("_receive_multi_node_messages", frame=frames[0].to_bytes())
            message = deserialize_message(frames[0].to_bytes(), messages.Message)
            specific_message_obj = self._get_and_check_specific_message_obj(message)
            self._check_sequence_number(specific_message_obj)
            position = self._get_and_check_multi_node_position(specific_message_obj, number_of_instances_in_cluster)
            self._check_if_position_is_already_set(position, result, specific_message_obj)
            result[position] = frames[1].to_bytes()
        positions_required_from_other_nodes = range(self._number_of_instances_per_node, number_of_instances_in_cluster)
        is_done = set(positions_required_from_other_nodes).issubset(result.keys())
        return is_done

    def _is_result_complete(self, result: Dict[int, bytes], number_of_instances_in_cluster: int) -> bool:
        complete = len(result) == number_of_instances_in_cluster
        return complete

    def _get_and_check_local_position(self, specific_message_obj: Gather) -> int:
        local_position = specific_message_obj.position
        if not (0 < local_position < self._number_of_instances_per_node):
            raise RuntimeError(
                f"Got message with not allowed position. "
                f"Position needs to be greater than 0 and smaller than {self._number_of_instances_per_node}, "
                f"but we got {local_position} in message {specific_message_obj}")
        return local_position

    def _get_and_check_multi_node_position(self, specific_message_obj: Gather,
                                           number_of_instances_in_cluster: int) -> int:
        position = specific_message_obj.position
        if not (self._number_of_instances_per_node <= position < number_of_instances_in_cluster):
            raise RuntimeError(
                f"Got message with not allowed position. "
                f"Position needs to be greater equal than {self._number_of_instances_per_node} and "
                f"smaller than {number_of_instances_in_cluster}, "
                f"but we got {position} in message {specific_message_obj}")
        return position

    def _check_sequence_number(self, specific_message_obj: Gather):
        if specific_message_obj.sequence_number != self._sequence_number:
            raise RuntimeError(
                f"Got message with different sequence number. "
                f"We expect the sequence number {self._sequence_number} "
                f"but we got {self._sequence_number} in message {specific_message_obj}")

    def _get_and_check_specific_message_obj(self, message: messages.Message) -> Gather:
        specific_message_obj = message.__root__
        if not isinstance(specific_message_obj, Gather):
            raise TypeError(f"Received the wrong message type. "
                            f"Expected {Gather.__name__} got {type(message)}. "
                            f"For message {message}.")
        return specific_message_obj

    def _check_if_position_is_already_set(self, position: int, result: Dict[int, bytes], specific_message_obj: Gather):
        if position in result:
            raise RuntimeError(f"Already received a message for position {position}. "
                               f"Got message {specific_message_obj}")
