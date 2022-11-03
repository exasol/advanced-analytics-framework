import random
import time
from pathlib import Path
from typing import Dict, Set

import structlog
from structlog import WriteLoggerFactory

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from tests.udf_communication.peer_communication.utils import TestProcess, BidirectionalQueue

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(file=Path(__file__).with_suffix(".log").open("wt")),
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(),
        structlog.processors.JSONRenderer()
    ]
)



def run(name: str, group_identifier: str, number_of_instances: int, queue: BidirectionalQueue):
    listen_ip = IPAddress(ip_address=f"127.1.0.1")
    com = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=listen_ip,
        group_identifier=group_identifier)
    queue.put(com.my_connection_info)
    peer_connection_infos = queue.get()
    for index, connection_infos in peer_connection_infos.items():
        com.register_peer(connection_infos)
        time.sleep(random.random() / 10)
    com.wait_for_peers()
    queue.put("Wait finish")
    for peer in com.peers():
        com.send(peer, [name.encode("utf8")])
    received_values: Set[str] = set()
    for peer in com.peers():
        value = com.recv(peer)
        received_values.add(value[0].decode("utf8"))
    queue.put(received_values)


def test():
    group = f"{time.monotonic_ns()}"
    number_of_instances = 10
    processes: Dict[int, TestProcess] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        processes[i] = TestProcess(f"t{i}", group, number_of_instances, run=run)
        processes[i].start()
        connection_infos[i] = processes[i].get()

    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)

    for i in range(number_of_instances):
        processes[i].get()

    received_values: Dict[int, Set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = processes[i].get()

    expected_received_values = {
        i: {
            thread.name
            for index, thread in processes.items()
            if index != i
        }
        for i in range(number_of_instances)
    }
    assert expected_received_values == received_values

    for i in range(number_of_instances):
        processes[i].join(timeout=5)
        assert not processes[i].is_alive()
