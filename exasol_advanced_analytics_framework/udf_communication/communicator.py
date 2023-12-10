from typing import Optional, List

import structlog
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.all_gather_operation import AllGatherOperation
from exasol_advanced_analytics_framework.udf_communication.broadcast_operation import BroadcastOperation
from exasol_advanced_analytics_framework.udf_communication.communicator_protocol import CommunicatorProtocol
from exasol_advanced_analytics_framework.udf_communication.discovery import localhost, multi_node
from exasol_advanced_analytics_framework.udf_communication.gather_operation import GatherOperation
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory

LOCALHOST_LEADER_RANK = 0
MULTI_NODE_LEADER_RANK = 0

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class Communicator(CommunicatorProtocol):

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
        self._logger = LOGGER.bind(
            name=self._name,
            node_name=self._node_name,
            instance_name=instance_name,
            listen_ip=self._listen_ip.dict(),
            localhost_listen_ip=self._localhost_listen_ip.dict(),
            multi_node_discovery_ip=self._multi_node_discovery_ip.dict(),
            multi_node_discovery_port=self._multi_node_discovery_port.dict(),
            is_discovery_leader_node=self._is_discovery_leader_node,
            group_identifier=self._group_identifier,
            number_of_nodes=self._number_of_nodes,
            number_of_instances_per_node=self._number_of_instances_per_node
        )
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
            self._logger.info("multi_node_communicator discovery start")
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
            self._logger.info("multi_nodet_communicator discovery end")
            return peer_communicator
        else:
            return None

    def _create_localhost_communicator(self) -> PeerCommunicator:
        localhost_group_identifier = f"{self._group_identifier}_{self._node_name}_local"
        localhost_name = f"{self._name}_local"
        discovery_socket_factory = localhost.DiscoverySocketFactory()
        self._logger.info("localhost_communicator discovery start")
        peer_communicator = self._localhost_communicator_factory.create(
            group_identifier=localhost_group_identifier,
            name=localhost_name,
            number_of_instances=self._number_of_instances_per_node,
            listen_ip=self._localhost_listen_ip,
            discovery_port=self._localhost_discovery_port,
            socket_factory=self._socket_factory,
            discovery_socket_factory=discovery_socket_factory)
        self._logger.info("localhost_communicator discovery end")
        return peer_communicator

    def gather(self, value: bytes) -> Optional[List[bytes]]:
        sequence_number = self._next_sequence_number()
        gather = GatherOperation(sequence_number=sequence_number, value=value,
                                 localhost_communicator=self._localhost_communicator,
                                 multi_node_communicator=self._multi_node_communicator,
                                 socket_factory=self._socket_factory,
                                 number_of_instances_per_node=self._number_of_instances_per_node)
        return gather()

    def broadcast(self, value: Optional[bytes]) -> bytes:
        sequence_number = self._next_sequence_number()
        broadcast = BroadcastOperation(sequence_number=sequence_number, value=value,
                                       localhost_communicator=self._localhost_communicator,
                                       multi_node_communicator=self._multi_node_communicator,
                                       socket_factory=self._socket_factory)
        return broadcast()

    def all_gather(self, value: bytes) -> List[bytes]:
        all_gather = AllGatherOperation(communicator=self, value=value)
        return all_gather()

    def is_multi_node_leader(self):
        if self._multi_node_communicator is not None:
            return self._multi_node_communicator.rank == MULTI_NODE_LEADER_RANK
        else:
            return self._localhost_communicator.rank == LOCALHOST_LEADER_RANK

    def stop(self):
        self._localhost_communicator.stop()
        if self._multi_node_communicator is not None:
            self._multi_node_communicator.stop()

    @property
    def listen_ip(self) -> IPAddress:
        return self._listen_ip


class CommunicatorFactory:

    def create(self,
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
               socket_factory: SocketFactory) -> Communicator:
        return Communicator(
            multi_node_discovery_ip=multi_node_discovery_ip,
            multi_node_discovery_port=multi_node_discovery_port,
            local_discovery_port=local_discovery_port,
            node_name=node_name,
            instance_name=instance_name,
            listen_ip=listen_ip,
            group_identifier=group_identifier,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
            is_discovery_leader_node=is_discovery_leader_node,
            socket_factory=socket_factory
        )
