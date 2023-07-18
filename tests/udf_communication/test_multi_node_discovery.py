import time
from pathlib import Path
from typing import Dict, List

import pytest
import structlog
import zmq
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.discovery import multi_node
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator import key_for_peer
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_wrapper import ZMQSocketFactory
from tests.udf_communication.peer_communication.conditional_method_dropper import ConditionalMethodDropper
from tests.udf_communication.peer_communication.utils import TestProcess, BidirectionalQueue, assert_processes_finish

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(file=Path(__file__).with_suffix(".log").open("wt")),
    processors=[
        structlog.contextvars.merge_contextvars,
        ConditionalMethodDropper(method_name="debug"),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.processors.ExceptionRenderer(exception_formatter=ExceptionDictTransformer(locals_max_string=320)),
        structlog.processors.CallsiteParameterAdder(),
        structlog.processors.JSONRenderer()
    ]
)

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


def run(name: str, group_identifier: str, number_of_instances: int, queue: BidirectionalQueue, seed: int = 0):
    listen_ip = IPAddress(ip_address=f"127.1.0.1")
    global_discovery_socket = multi_node.DiscoverySocket(ip_address=listen_ip, port=Port(port=44444))
    context = zmq.Context()
    socket_factory = ZMQSocketFactory(context)
    leader = False
    leader_name = "t0"
    if name == leader_name:
        leader = True
    peer_communicator = PeerCommunicator(
        name=name,
        number_of_peers=number_of_instances,
        listen_ip=listen_ip,
        group_identifier=group_identifier,
        is_leader=leader,
        forward_enabled=True,
        socket_factory=socket_factory
    )
    queue.put(peer_communicator.my_connection_info)
    discovery = multi_node.DiscoveryStrategy(
        discovery_timeout_in_seconds=120,
        time_between_ping_messages_in_seconds=1,
        discovery_socket=global_discovery_socket,
        peer_communicator=peer_communicator
    )
    if peer_communicator.are_all_peers_connected():
        peers = peer_communicator.peers()
        queue.put(peers)
    else:
        queue.put([])


@pytest.mark.parametrize("number_of_instances, repetitions", [(2, 1000), (10, 100)])
def test_reliability(number_of_instances: int, repetitions: int):
    run_test_with_repetitions(number_of_instances, repetitions)


REPETITIONS_FOR_FUNCTIONALITY = 2


def test_functionality_2():
    run_test_with_repetitions(2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_3():
    run_test_with_repetitions(3, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_5():
    run_test_with_repetitions(5, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_25():
    run_test_with_repetitions(25, REPETITIONS_FOR_FUNCTIONALITY)


def run_test_with_repetitions(number_of_instances: int, repetitions: int):
    for i in range(repetitions):
        LOGGER.info(f"Start iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    number_of_instances=number_of_instances)
        start_time = time.monotonic()
        group = f"{time.monotonic_ns()}"
        expected_peers_of_threads, peers_of_threads = run_test(group, number_of_instances)
        assert expected_peers_of_threads == peers_of_threads
        end_time = time.monotonic()
        LOGGER.info(f"Finish iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    number_of_instances=number_of_instances,
                    duration=end_time - start_time)


def run_test(group: str, number_of_instances: int):
    connection_infos: Dict[int, ConnectionInfo] = {}
    processes: List[TestProcess] = [TestProcess(f"t{i}", group, number_of_instances, run=run)
                                    for i in range(number_of_instances)]
    for i in range(number_of_instances):
        processes[i].start()
        connection_infos[i] = processes[i].get()
    assert_processes_finish(processes, timeout_in_seconds=180)
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