import time
from pathlib import Path
from typing import Dict, List

import structlog
from structlog import WriteLoggerFactory
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.local_discovery_socket import LocalDiscoverySocket
from exasol_advanced_analytics_framework.udf_communication.local_discovery_strategy import LocalDiscoveryStrategy
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
    local_discovery_socket = LocalDiscoverySocket(Port(port=44444))
    peer_communicator = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=IPAddress(ip_address="127.0.0.1"),
        group_identifier=group_identifier
    )
    queue.put(peer_communicator.my_connection_info)
    discovery = LocalDiscoveryStrategy(
        discovery_timeout_in_seconds=10,
        time_between_ping_messages_in_seconds=1,
        local_discovery_socket=local_discovery_socket,
        peer_communicator=peer_communicator
    )
    if peer_communicator.are_all_peers_connected():
        peers = peer_communicator.peers()
        queue.put(peers)
    else:
        queue.put([])


def test():
    group = f"{time.monotonic_ns()}"
    logger = LOGGER.bind(group=group, location="test")
    logger.info("start")
    number_of_instances = 10
    connection_infos: Dict[int, ConnectionInfo] = {}
    processes: List[TestProcess] = [TestProcess(f"t{i}", group, number_of_instances, run=run)
                                    for i in range(number_of_instances)]
    for i in range(number_of_instances):
        processes[i].start()
        connection_infos[i] = processes[i].get()

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
    if not expected_peers_of_threads == peers_of_threads:
        logger.info("failed", reason=f"Did not get expected_peers_of_threads",
                    expected_peers_of_threads=expected_peers_of_threads,
                    peers_of_threads=peers_of_threads)

    assert expected_peers_of_threads == peers_of_threads

    logger.info("success")
