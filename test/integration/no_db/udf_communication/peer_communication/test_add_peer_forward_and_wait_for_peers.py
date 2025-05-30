import time
import traceback
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
from numpy.random import RandomState
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import (
    ForwardRegisterPeerConfig,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator import (
    key_for_peer,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.socket_factory.fault_injection import (
    FaultInjectionSocketFactory,
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
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.processors.ExceptionRenderer(
            exception_formatter=ExceptionDictTransformer(locals_max_string=320)
        ),
        structlog.processors.CallsiteParameterAdder(),
        structlog.processors.JSONRenderer(),
    ],
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


def run(parameter: PeerCommunicatorTestProcessParameter, queue: BidirectionalQueue):
    logger = LOGGER.bind(
        group_identifier=parameter.group_identifier, name=parameter.instance_name
    )
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        context = zmq.Context()
        socket_factory = ZMQSocketFactory(context)
        socket_factory = FaultInjectionSocketFactory(
            socket_factory, 0.01, RandomState(parameter.seed)
        )
        leader = False
        leader_name = "i0"
        leader = parameter.instance_name == leader_name
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=leader, is_enabled=True
                ),
            ),
            socket_factory=socket_factory,
        )
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            if parameter.instance_name == leader_name:
                for index, connection_info in peer_connection_infos.items():
                    com.register_peer(connection_info)
            peers = com.peers(timeout_in_milliseconds=None)
            logger.info("peers", peers=len(peers))
        finally:
            logger.info("com stop before")
            com.stop()
            logger.info("com stop after")
        queue.put(peers)
    except Exception as e:
        traceback.print_exc()
        logger.exception("Exception during test", exception=e)
        queue.put([])


@pytest.mark.parametrize(
    "number_of_instances, repetitions", [(2, 1000), (10, 100), (25, 10)]
)
def test_reliability(number_of_instances: int, repetitions: int):
    run_test_with_repetitions(number_of_instances, repetitions)


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2():
    run_test_with_repetitions(2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_3():
    run_test_with_repetitions(3, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_5():
    run_test_with_repetitions(5, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_15():
    run_test_with_repetitions(15, REPETITIONS_FOR_FUNCTIONALITY)


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
            group, number_of_instances, seed=i
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


def run_test(group: str, number_of_instances: int, seed: int):
    connection_infos: Dict[int, ConnectionInfo] = {}
    parameters = [
        PeerCommunicatorTestProcessParameter(
            instance_name=f"i{i}",
            group_identifier=group,
            number_of_instances=number_of_instances,
            seed=seed + i,
        )
        for i in range(number_of_instances)
    ]
    processes: List[TestProcess[PeerCommunicatorTestProcessParameter]] = [
        TestProcess(parameter, run=run) for parameter in parameters
    ]
    for i in range(number_of_instances):
        processes[i].start()
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=300)
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
