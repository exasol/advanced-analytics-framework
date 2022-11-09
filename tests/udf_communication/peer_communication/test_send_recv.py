import random
import time
from pathlib import Path
from typing import Dict, Set, List

import pytest
import structlog
from structlog import WriteLoggerFactory

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from tests.udf_communication.peer_communication.utils import TestProcess, BidirectionalQueue, assert_processes_finish

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
    com.wait_for_peers()
    for peer in com.peers():
        com.send(peer, [name.encode("utf8")])
    received_values: Set[str] = set()
    for peer in com.peers():
        value = com.recv(peer)
        received_values.add(value[0].decode("utf8"))
    queue.put(received_values)


@pytest.mark.repeat(1000)
@pytest.mark.parametrize("number_of_instances", [2, 10, 50])
def test_reliability(number_of_instances: int):
    group = f"{time.monotonic_ns()}"
    expected_received_values, received_values = run_test(group, number_of_instances)
    assert expected_received_values == received_values


def test_functionality():
    group = f"{time.monotonic_ns()}"
    number_of_instances = 2
    expected_received_values, received_values = run_test(group, number_of_instances)
    assert expected_received_values == received_values


def run_test(group: str, number_of_instances: int):
    connection_infos: Dict[int, ConnectionInfo] = {}
    processes: List[TestProcess] = [TestProcess(f"t{i}", group, number_of_instances, run=run)
                                    for i in range(number_of_instances)]
    for i in range(number_of_instances):
        processes[i].start()
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=120)
    received_values: Dict[int, Set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = processes[i].get()
    expected_received_values = {
        i: {
            thread.name
            for index, thread in enumerate(processes)
            if index != i
        }
        for i in range(number_of_instances)
    }
    return expected_received_values, received_values
