import time
from pathlib import Path
from test.integration.no_db.udf_communication.peer_communication.conditional_method_dropper import (
    ConditionalMethodDropper,
)
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)
from typing import (
    Dict,
    List,
)

import pytest
import structlog
import zmq
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.discovery import localhost
from exasol.analytics.udf.communication.discovery.localhost.communicator import (
    CommunicatorFactory,
)
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.peer_communicator import (
    key_for_peer,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(
        file=Path(__file__).with_suffix(".log").open("wt")
    ),
    processors=[
        structlog.contextvars.merge_contextvars,
        ConditionalMethodDropper(method_name="debug"),
        ConditionalMethodDropper(method_name="info"),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.processors.ExceptionRenderer(
            exception_formatter=ExceptionDictTransformer(locals_max_string=320)
        ),
        structlog.processors.CallsiteParameterAdder(),
        structlog.processors.JSONRenderer(),
    ],
)

LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


def run(parameter: PeerCommunicatorTestProcessParameter, queue: BidirectionalQueue):
    discovery_port = Port(port=44444)
    listen_ip = IPAddress(ip_address="127.1.0.1")
    context = zmq.Context()
    socket_factory = ZMQSocketFactory(context)
    discovery_socket_factory = localhost.DiscoverySocketFactory()
    peer_communicator = CommunicatorFactory().create(
        group_identifier=parameter.group_identifier,
        name=parameter.instance_name,
        number_of_instances=parameter.number_of_instances,
        listen_ip=listen_ip,
        discovery_port=discovery_port,
        socket_factory=socket_factory,
        discovery_socket_factory=discovery_socket_factory,
    )
    queue.put(peer_communicator.my_connection_info)
    if peer_communicator.are_all_peers_connected():
        peers = peer_communicator.peers()
        queue.put(peers)
    else:
        queue.put([])


@pytest.mark.parametrize(
    "number_of_instances, repetitions", [(2, 1000), (10, 100), (25, 10)]
)
def test_reliability(number_of_instances: int, repetitions: int):
    run_test_with_repetitions(number_of_instances, repetitions)


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2():
    run_test_with_repetitions(2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_25():
    run_test_with_repetitions(25, REPETITIONS_FOR_FUNCTIONALITY)


def run_test_with_repetitions(number_of_instances: int, repetitions: int):
    for i in range(repetitions):
        LOGGER.info(
            f"Start iteration",
            iteration=i + 1,
            repetitions=repetitions,
            number_of_instances=number_of_instances,
        )
        start_time = time.monotonic()
        group = f"{time.monotonic_ns()}"
        expected_peers_of_threads, peers_of_threads = run_test(
            group, number_of_instances
        )
        assert expected_peers_of_threads == peers_of_threads
        end_time = time.monotonic()
        LOGGER.info(
            f"Finish iteration",
            iteration=i + 1,
            repetitions=repetitions,
            number_of_instances=number_of_instances,
            duration=end_time - start_time,
        )


def run_test(group: str, number_of_instances: int):
    connection_infos: Dict[int, ConnectionInfo] = {}
    parameters = [
        PeerCommunicatorTestProcessParameter(
            instance_name=f"i{i}",
            group_identifier=group,
            number_of_instances=number_of_instances,
            seed=0,
        )
        for i in range(number_of_instances)
    ]
    processes: List[TestProcess[PeerCommunicatorTestProcessParameter]] = [
        TestProcess(parameter, run=run) for parameter in parameters
    ]
    for i in range(number_of_instances):
        processes[i].start()
    for i in range(number_of_instances):
        connection_infos[i] = processes[i].get()
    assert_processes_finish(processes, timeout_in_seconds=180)
    peers_of_threads: Dict[int, List[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        peers_of_threads[i] = processes[i].get()
    expected_peers_of_threads = {
        i: sorted(
            [
                Peer(connection_info=connection_info)
                for index, connection_info in connection_infos.items()
            ],
            key=key_for_peer,
        )
        for i in range(number_of_instances)
    }
    return expected_peers_of_threads, peers_of_threads
