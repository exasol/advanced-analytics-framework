from typing import Optional, List, Any

from exasol_advanced_analytics_framework.udf_communication.discovery import localhost, multi_node
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import Gather
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, Frame


class GatherRunner:

    def __init__(self,
                 sequence_number: int,
                 value: bytes,
                 localhost_communicator: PeerCommunicator,
                 multi_node_communicator: PeerCommunicator,
                 socket_factory: SocketFactory,
                 number_of_instances_per_node: int):
        self._socket_factory = socket_factory
        self._value = value
        self._sequence_number = sequence_number
        self._multi_node_communicator = multi_node_communicator
        self._localhost_communicator = localhost_communicator

    def __call__(self) -> Optional[List[bytes]]:
        if self._localhost_communicator.rank > 0:
            return self._send_to_localhost_leader()
        else:
            return self._handle_messages_from_local_peers()

    def _send_to_localhost_leader(self) -> None:
        peers = self._localhost_communicator.peers()
        leader = peers[0]
        position = self._localhost_communicator.rank
        source = self._localhost_communicator.peer
        value_frame = self._socket_factory.create_frame(self._value)
        frames = self._construct_gather_message(leader=leader, source=source,
                                                position=position, value_frame=value_frame)
        self._localhost_communicator.send(peer=leader, message=frames)
        return None

    def _handle_messages_from_local_peers(self) -> Optional[List[bytes]]:
        if self._multi_node_communicator.rank > 0:
            return self._forward_to_multi_node_leader()
        else:
            return self._handle_messages_from_all_nodes()

    def _forward_to_multi_node_leader(self) -> None:
        self._send_local_leader_message_to_multi_node_leader()
        message_received_from = {self._localhost_communicator.peer}
        peers = set(self._localhost_communicator.peers())
        while message_received_from != peers:
            # TODO poll
            # TODO receive
            pass
        return None

    def _send_local_leader_message_to_multi_node_leader(self):
        local_position = 0
        value_frame = self._socket_factory.create_frame(self._value)
        self.send_to_multi_node_leader(local_position=local_position, value_frame=value_frame)

    def send_to_multi_node_leader(self, local_position: int, value_frame: Frame):
        peers = self._multi_node_communicator.peers()
        leader = peers[0]
        source = self._multi_node_communicator.peer
        base_position = self._multi_node_communicator.rank * self._localhost_communicator.number_of_peers
        position = base_position + local_position
        frames = self._construct_gather_message(leader=leader, source=source,
                                                position=position, value_frame=value_frame)
        self._multi_node_communicator.send(peer=leader, message=frames)

    def _construct_gather_message(self, leader, position, source, value_frame):
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
        return


class Communicator:

    def __init__(self,
                 multi_node_discovery_ip: IPAddress,
                 multi_node_discovery_port: Port,
                 local_discovery_port: Port,
                 node_name: str,
                 instance_name: str,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 number_of_nodes: int,
                 number_of_instances_per_node: int,
                 is_discovery_leader_node: bool,
                 socket_factory: SocketFactory,
                 localhost_communicator_factory: localhost.CommunicatorFactory = localhost.CommunicatorFactory(),
                 multi_node_communicator_factory: multi_node.CommunicatorFactory = multi_node.CommunicatorFactory(),
                 ):
        self._number_of_nodes = number_of_nodes
        self._number_of_instances_per_node = number_of_instances_per_node
        self._group_identifier = group_identifier
        self._node_name = node_name
        self._multi_node_communicator_factory = multi_node_communicator_factory
        self._localhost_communicator_factory = localhost_communicator_factory
        self._socket_factory = socket_factory
        self._is_discovery_leader_node = is_discovery_leader_node
        self._multi_node_discovery_ip = multi_node_discovery_ip
        self._multi_node_discovery_port = multi_node_discovery_port
        self._localhost_discovery_port = local_discovery_port
        self._listen_ip = listen_ip
        self._localhost_listen_ip = IPAddress(ip_address="127.1.0.1")
        self._name = f"{node_name}_{instance_name}"
        self._localhost_communicator = self._create_localhost_communicator()
        self._multi_node_communicator = self._create_multi_node_communicator()
        self._sequence_number = 0

    def _next_sequence_number(self) -> int:
        result = self._sequence_number
        self._sequence_number += 1
        return result

    def _create_multi_node_communicator(self) -> Optional[PeerCommunicator]:
        multi_node_name = f"{self._name}_global"
        multi_node_group_identifier = f"{self._group_identifier}_global"
        if self._localhost_communicator.rank == 0:
            discovery_socket_factory = multi_node.DiscoverySocketFactory()
            is_discovery_leader = self._localhost_communicator.rank == 0 and self._is_discovery_leader_node
            peer_communicator = self._multi_node_communicator_factory.create(
                group_identifier=multi_node_group_identifier,
                name=multi_node_name,
                number_of_instances=self._number_of_nodes,
                is_discovery_leader=is_discovery_leader,
                listen_ip=self._listen_ip,
                discovery_ip=self._multi_node_discovery_ip,
                discovery_port=self._multi_node_discovery_port,
                socket_factory=self._socket_factory,
                discovery_socket_factory=discovery_socket_factory)
            return peer_communicator
        else:
            return None

    def _create_localhost_communicator(self) -> PeerCommunicator:
        localhost_group_identifier = f"{self._group_identifier}_{self._node_name}_local"
        localhost_name = f"{self._name}_local"
        discovery_socket_factory = localhost.DiscoverySocketFactory()
        peer_communicator = self._localhost_communicator_factory.create(
            group_identifier=localhost_group_identifier,
            name=localhost_name,
            number_of_instances=self._number_of_instances_per_node,
            listen_ip=self._localhost_listen_ip,
            discovery_port=self._localhost_discovery_port,
            socket_factory=self._socket_factory,
            discovery_socket_factory=discovery_socket_factory)
        return peer_communicator

    def gather(self, value: bytes) -> Optional[List[bytes]]:
        sequence_number = self._next_sequence_number()
        gather = GatherRunner(sequence_number=sequence_number, value=value,
                              localhost_communicator=self._localhost_communicator,
                              multi_node_communicator=self._multi_node_communicator,
                              socket_factory=self._socket_factory)
        return gather()
