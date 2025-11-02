import os
import sys
import time
import traceback
from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)

import pytest
import structlog
import zmq
from numpy.random import RandomState
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import (
    ForwardRegisterPeerConfig,
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

configure_structlog(__file__)

LOGGER: FilteringBoundLogger = structlog.get_logger()


def run(parameter: PeerCommunicatorTestProcessParameter, queue: BidirectionalQueue):
    logger = LOGGER.bind(
        group_identifier=parameter.group_identifier, name=parameter.instance_name
    )
    received_values: set[str] = set()
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        context = zmq.Context()
        socket_factory = ZMQSocketFactory(context)
        socket_factory = FaultInjectionSocketFactory(
            socket_factory, 0.01, RandomState(parameter.seed)
        )
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            socket_factory=socket_factory,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=False, is_enabled=False
                ),
            ),
        )
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_infos in peer_connection_infos.items():
                com.register_peer(connection_infos)
            com.wait_for_peers()
            LOGGER.info("Peer is ready", name=parameter.instance_name)
            for peer in com.peers():
                if peer != Peer(connection_info=com.my_connection_info):
                    com.send(
                        peer,
                        [
                            socket_factory.create_frame(
                                parameter.instance_name.encode("utf8")
                            )
                        ],
                    )
            for peer in com.peers():
                if peer != Peer(connection_info=com.my_connection_info):
                    value = com.recv(peer)
                    received_values.add(value[0].to_bytes().decode("utf8"))
        finally:
            try:
                com.stop()
            except Exception as e:
                logger.exception("Exception during stop")
                queue.put(f"Failed: {e}")
            context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        logger.exception("Exception during test")
        queue.put(f"Failed: {e}")
    queue.put(received_values)


@pytest.mark.parametrize("number_of_instances, repetitions", [(2, 1000), (10, 100)])
def test_reliability(number_of_instances: int, repetitions: int):
    run_test_with_repetitions(number_of_instances, repetitions)


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2():
    run_test_with_repetitions(2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_5():
    run_test_with_repetitions(5, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


@pytest.mark.skipif(
    "GITHUB_ACTIONS" in os.environ,
    reason="This test is unstable on Github Action, "
    "because of the limited number of cores on the default runners.",
)
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
    connection_infos: dict[int, ConnectionInfo] = {}
    parameters = [
        PeerCommunicatorTestProcessParameter(
            instance_name=f"i{i}",
            group_identifier=group,
            number_of_instances=number_of_instances,
            seed=seed + i,
        )
        for i in range(number_of_instances)
    ]
    processes: list[TestProcess[PeerCommunicatorTestProcessParameter]] = [
        TestProcess(parameter, run=run) for parameter in parameters
    ]
    for i in range(number_of_instances):
        processes[i].start()
    for i in range(number_of_instances):
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=180)
    received_values: dict[int, set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = processes[i].get()
    expected_received_values = {
        i: {
            thread.parameter.instance_name
            for index, thread in enumerate(processes)
            if index != i
        }
        for i in range(number_of_instances)
    }
    return expected_received_values, received_values
