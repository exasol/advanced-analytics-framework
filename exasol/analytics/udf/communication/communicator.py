from typing import List, Optional

from exasol.analytics.udf.communication.broadcast_operation import BroadcastOperation
from exasol.analytics.udf.communication.discovery import localhost, multi_node
from exasol.analytics.udf.communication.gather_operation import GatherOperation
from exasol.analytics.udf.communication.ip_address import IPAddress, Port
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.socket_factory.abstract import SocketFactory

LOCALHOST_LEADER_RANK = 0
MULTI_NODE_LEADER_RANK = 0


class Communicator:

    def __init__(
        self,
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
        sequence_number = self._sequence_number
        self._sequence_number += 1
        return sequence_number

    def _create_multi_node_communicator(self) -> Optional[PeerCommunicator]:
        multi_node_name = f"{self._name}_global"
        multi_node_group_identifier = f"{self._group_identifier}_global"
        if self._localhost_communicator.rank == LOCALHOST_LEADER_RANK:
            discovery_socket_factory = multi_node.DiscoverySocketFactory()
            is_discovery_leader = (
                self._localhost_communicator.rank == LOCALHOST_LEADER_RANK
                and self._is_discovery_leader_node
            )
            peer_communicator = self._multi_node_communicator_factory.create(
                group_identifier=multi_node_group_identifier,
                name=multi_node_name,
                number_of_instances=self._number_of_nodes,
                is_discovery_leader=is_discovery_leader,
                listen_ip=self._listen_ip,
                discovery_ip=self._multi_node_discovery_ip,
                discovery_port=self._multi_node_discovery_port,
                socket_factory=self._socket_factory,
                discovery_socket_factory=discovery_socket_factory,
            )
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
            discovery_socket_factory=discovery_socket_factory,
        )
        return peer_communicator

    def gather(self, value: bytes) -> Optional[List[bytes]]:
        sequence_number = self._next_sequence_number()
        gather = GatherOperation(
            sequence_number=sequence_number,
            value=value,
            localhost_communicator=self._localhost_communicator,
            multi_node_communicator=self._multi_node_communicator,
            socket_factory=self._socket_factory,
            number_of_instances_per_node=self._number_of_instances_per_node,
        )
        return gather()

    def broadcast(self, value: Optional[bytes]) -> bytes:
        sequence_number = self._next_sequence_number()
        operation = BroadcastOperation(
            sequence_number=sequence_number,
            value=value,
            localhost_communicator=self._localhost_communicator,
            multi_node_communicator=self._multi_node_communicator,
            socket_factory=self._socket_factory,
        )
        return operation()

    def is_multi_node_leader(self):
        if self._multi_node_communicator is not None:
            return self._multi_node_communicator.rank == MULTI_NODE_LEADER_RANK
        else:
            return self._localhost_communicator.rank == LOCALHOST_LEADER_RANK
