import socket
import time
from typing import List, Tuple, Protocol

import structlog
from pydantic import BaseModel
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import udf_communicator

LOGGER: FilteringBoundLogger = structlog.get_logger()


class WorkerAddress(BaseModel):
    ip_address: IPAddress
    port: Port


class ClusterInformation(BaseModel):
    workers: List[WorkerAddress]


def reserve_port(ip: IPAddress) -> Port:
    def new_socket():
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def bind(sock: socket.socket, ip: IPAddress, port: int):
        sock.bind((ip.ip_address, port))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def acquire_port_number(sock: socket.socket, ip: IPAddress) -> int:
        bind(sock, ip, 0)
        return sock.getsockname()[1]

    with new_socket() as sock:
        port_number = acquire_port_number(sock, ip)
        port = Port(port=port_number)
        LOGGER.info("reserve_port", ip=ip, port=port)
        return port


def exchange_cluster_information(communicator: Communicator, worker_address: WorkerAddress) \
        -> ClusterInformation:
    LOGGER.info("before gather", worker_address=worker_address)
    workers_messages = communicator.gather(worker_address.json().encode("UTF-8"))
    LOGGER.info("after gather", worker_address=worker_address)
    broadcast_value = None
    if communicator.is_multi_node_leader():
        workers = [WorkerAddress.parse_raw(message.decode("UTF-8")) for message in workers_messages]
        cluster_information = ClusterInformation(workers=workers)
        broadcast_value = cluster_information.json().encode("UTF-8")
    LOGGER.info("before broadcast", worker_address=worker_address)
    broadcast_result = communicator.broadcast(broadcast_value)
    LOGGER.info("after broadcast", worker_address=worker_address)

    # if communicator.is_multi_node_leader():
    #    time.sleep(5)

    cluster_information = ClusterInformation.parse_raw(broadcast_result.decode("UTF-8"))
    return cluster_information


class UDFCommunicatorFactory(Protocol):

    def create(self) -> Communicator:
        pass


class DistributedUDF(Protocol):
    def run(self, ctx, exa, udf_communicator_factory: UDFCommunicatorFactory):
        pass


class _RunnerUDFCommunicatorFactory:
    def __init__(self, exa, connection_name: str):
        self._connection_name = connection_name
        self._exa = exa

    def create(self) -> Communicator:
        return udf_communicator(self._exa, self._connection_name)


class DistributedUDFRunner:

    def __init__(self, distributed_udf: DistributedUDF):
        self._distributed_udf = distributed_udf

    def run(self, ctx, exa, connection_name: str):
        factory = _RunnerUDFCommunicatorFactory(exa=exa, connection_name=connection_name)
        self._distributed_udf.run(ctx, exa, factory)
