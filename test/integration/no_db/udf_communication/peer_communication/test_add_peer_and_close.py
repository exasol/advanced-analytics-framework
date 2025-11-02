import sys
import time
import traceback
from datetime import timedelta
from test.integration.no_db.peer_com_runner import (
    PeerCommunicatorFactory,
    RepetitionRunner,
    expect_success,
)
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
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
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


def executor(
    logger: FilteringBoundLogger,
    communicator_factory: PeerCommunicatorFactory,
    parameter: PeerCommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    try:
        setup = communicator_factory.create(parameter)
        com = setup.communicator
        try:
            queue.put(com.my_connection_info)
            for index, connection_info in queue.get().items():
                com.register_peer(connection_info)
        finally:
            try:
                com.stop()
                queue.put("Success")
            except:
                logger.exception("Exception during stop")
                queue.put("Failed")
            setup.context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        queue.put("Failed")
        logger.exception("Exception during test")


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=PeerCommunicatorFactory(inject_faults=True),
    executor=executor,
    expectation_generator=expect_success,
)


@pytest.mark.parametrize("instances", [2, 3, 10, 25])
def test_functionality_new(instances):
    RUNNER.run_multiple(instances, repetitions=1)


@pytest.mark.parametrize(
    "instances, repetitions", [(2, 1000), (10, 100), (25, 10)]
)
def test_reliability_new(instances, repetitions):
    RUNNER.run_multiple(instances, repetitions)


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
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            socket_factory=socket_factory,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=False, is_enabled=False
                )
            ),
        )
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_info in peer_connection_infos.items():
                com.register_peer(connection_info)
        finally:
            try:
                com.stop()
                queue.put("Success")
            except:
                logger.exception("Exception during stop")
                queue.put("Failed")
            context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        queue.put("Failed")
        logger.exception("Exception during test")


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


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_25():
    run_test_with_repetitions(25, REPETITIONS_FOR_FUNCTIONALITY)


# identical
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


# identical
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
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=180)
    result_of_threads: dict[int, list[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        result_of_threads[i] = processes[i].get()
    expected_results_of_threads = {i: "Success" for i in range(number_of_instances)}
    return expected_results_of_threads, result_of_threads
