import zmq

from exasol_advanced_analytics_framework.udf_communication.global_discovery_socket import GlobalDiscoverySocketFactory
from exasol_advanced_analytics_framework.udf_communication.global_peer_communicator import \
    create_global_peer_communicator
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocketFactory
from exasol_advanced_analytics_framework.udf_communication.local_peer_communicator import create_local_peer_communicator
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_socket_factory import ZMQSocketFactory


class Communicator:

    def __init__(self,
                 global_discovery_port: Port,
                 local_discovery_port: Port,
                 global_discovery_ip: IPAddress,
                 node_name: str,
                 instance_name: str,
                 listen_ip: IPAddress,
                 group_identifier: str,
                 number_of_nodes: int,
                 number_of_instances_per_node: int,
                 is_discovery_leader_node: bool):
        self._is_discovery_leader_node = is_discovery_leader_node
        self._global_discovery_ip = global_discovery_ip
        self._global_discovery_port = global_discovery_port
        self._local_discovery_port = local_discovery_port
        self._listen_ip = listen_ip
        self._local_listen_ip = IPAddress(ip_address="127.1.0.1")
        self._number_of_instances_per_node = number_of_instances_per_node
        self._context = zmq.Context()
        self._socket_factory = ZMQSocketFactory(self._context)
        self._name = f"{node_name}_{instance_name}"
        self._local_group_identifier = f"{group_identifier}_{node_name}_local"
        self._local_name = f"{self._name}_local"
        self._local_peer_communicator = self._create_local_peer_communicator()
        self._local_peer = Peer(connection_info=self._local_peer_communicator.my_connection_info)
        self._local_peers = self._local_peer_communicator.peers()
        self._local_rank = self._local_peers.index(self._local_peer)
        self._global_name = f"{self._name}_global"
        self._global_group_identifier = f"{group_identifier}_global"
        self._number_of_nodes = number_of_nodes
        self._global_peer_communicator = self._create_global_peer_communicator()
        if not self._global_peer_communicator is None:
            self._global_peer = Peer(connection_info=self._global_peer_communicator.my_connection_info)
            self._global_peers = self._local_peer_communicator.peers()
            self._global_rank = self._local_peers.index(self._local_peer)

    def _create_global_peer_communicator(self):
        if self._local_rank == 0:
            global_discovery_socket_factory = GlobalDiscoverySocketFactory()
            is_discovery_leader = self._local_rank == 0 and self._is_discovery_leader_node
            global_peer_communicator = create_global_peer_communicator(
                group_identifier=self._global_group_identifier,
                name=self._global_name,
                number_of_instances=self._number_of_nodes,
                is_discovery_leader=is_discovery_leader,
                listen_ip=self._listen_ip,
                discovery_ip=self._global_discovery_ip,
                discovery_port=self._global_discovery_port,
                socket_factory=self._socket_factory,
                global_discovery_socket_factory=global_discovery_socket_factory)
            return global_peer_communicator

    def _create_local_peer_communicator(self) -> PeerCommunicator:
        local_discovery_socket_factory = LocalDiscoverySocketFactory()
        local_peer_communicator = create_local_peer_communicator(
            group_identifier=self._local_group_identifier,
            name=self._local_name,
            number_of_instances=self._number_of_instances_per_node,
            listen_ip=self._local_listen_ip,
            discovery_port=self._local_discovery_port,
            socket_factory=self._socket_factory,
            local_discovery_socket_factory=local_discovery_socket_factory)
        return local_peer_communicator
