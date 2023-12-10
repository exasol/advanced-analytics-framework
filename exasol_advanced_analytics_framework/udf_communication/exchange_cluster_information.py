import contextlib
import socket
from typing import List, Iterator

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator
from exasol_advanced_analytics_framework.udf_communication.distributed_udf import LOGGER
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


class WorkerAddress(BaseModel):
    ip_address: IPAddress
    port: Port


class ClusterInformation(BaseModel):
    workers: List[WorkerAddress]


@contextlib.contextmanager
def reserve_port(ip: IPAddress) -> Iterator[Port]:
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
        yield port


def exchange_cluster_information(communicator: Communicator, worker_address: WorkerAddress) \
        -> ClusterInformation:
    worker_messages = communicator.all_gather(serialize_message(worker_address))
    worker_addresses = [deserialize_message(message, WorkerAddress) for message in worker_messages]
    cluster_information = ClusterInformation(workers=worker_addresses)
    return cluster_information
