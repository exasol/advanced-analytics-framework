import time
import traceback
from pathlib import Path
from typing import Dict, List

import pytest
import structlog
from structlog import WriteLoggerFactory
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator import key_for_peer
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

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


def run(name: str, group_identifier: str, number_of_instances: int, queue: BidirectionalQueue):
    logger = LOGGER.bind(group_identifier=group_identifier, name=name)
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        com = PeerCommunicator(
            name=name,
            number_of_peers=number_of_instances,
            listen_ip=listen_ip,
            group_identifier=group_identifier)
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_info in peer_connection_infos.items():
                com.register_peer(connection_info)
            peers = com.peers(timeout_in_milliseconds=None)
            logger.info("peers", peers=peers)
            queue.put(peers)
        finally:
            com.close()
    except Exception as e:
        traceback.print_exc()
        logger.exception("Exception during test", exception=e)


@pytest.mark.parametrize("number_of_instances, repetitions", [(2, 1000), (10, 100), (50, 10)])
def test_reliability(number_of_instances: int, repetitions: int):
    for i in range(repetitions):
        group = f"{time.monotonic_ns()}"
        expected_peers_of_threads, peers_of_threads = run_test(group, number_of_instances)
        assert expected_peers_of_threads == peers_of_threads


def test_functionality():
    group = f"{time.monotonic_ns()}"
    logger = LOGGER.bind(group=group, location="test")
    logger.info("start")
    number_of_instances = 2
    expected_peers_of_threads, peers_of_threads = run_test(group, number_of_instances)
    assert expected_peers_of_threads == peers_of_threads
    logger.info("success")


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
    peers_of_threads: Dict[int, List[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        peers_of_threads[i] = processes[i].get()
    expected_peers_of_threads = {
        i: sorted([
            Peer(connection_info=connection_info)
            for index, connection_info in connection_infos.items()
            if index != i
        ], key=key_for_peer)
        for i in range(number_of_instances)
    }
    return expected_peers_of_threads, peers_of_threads
