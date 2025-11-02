import time
from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_com_runner import (
    UdfCommunicatorFactory,
    RepetitionRunner,
)
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    CommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)

import pytest
import structlog
import zmq
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.communicator import Communicator
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)

configure_structlog(__file__)


def executor(
    logger: FilteringBoundLogger,
    communicator_factory: UdfCommunicatorFactory,
    parameter: CommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    communicator = communicator_factory.create(parameter)
    queue.put("Finished")


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=UdfCommunicatorFactory(),
    executor=executor,
    expect="Finished",
)

@pytest.mark.parametrize("nodes, instances_per_node", [
    (2,1), (1,2), (2, 2), (3,3),
])
def test_functionality_new(nodes, instances_per_node):
    RUNNER.run_multiple(nodes, instances_per_node, 1)


@pytest.mark.parametrize(
    "nodes, instances_per_node, repetitions",
    [
        (2, 2, 100),
        (3, 3, 20),
    ],
)
def test_reliability_new(nodes: int, instances_per_node: int, repetitions: int):
    RUNNER.run_multiple(
        number_of_nodes=nodes,
        number_of_instances_per_node=instances_per_node,
        repetitions=repetitions,
    )


LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)

def run(parameter: CommunicatorTestProcessParameter, queue: BidirectionalQueue):
    is_discovery_leader_node = parameter.node_name == "n0"
    context = zmq.Context()
    socket_factory = ZMQSocketFactory(context)
    communicator = Communicator(
        multi_node_discovery_port=Port(port=44444),
        local_discovery_port=parameter.local_discovery_port,
        multi_node_discovery_ip=IPAddress(ip_address="127.0.0.1"),
        node_name=parameter.node_name,
        instance_name=parameter.instance_name,
        listen_ip=IPAddress(ip_address="127.0.0.1"),
        group_identifier=parameter.group_identifier,
        number_of_nodes=parameter.number_of_nodes,
        number_of_instances_per_node=parameter.number_of_instances_per_node,
        is_discovery_leader_node=is_discovery_leader_node,
        socket_factory=socket_factory,
    )
    queue.put("Finished")


@pytest.mark.parametrize(
    "number_of_nodes, number_of_instances_per_node, repetitions",
    [
        (2, 2, 100),
        (3, 3, 20),
    ],
)
def test_reliability(
    number_of_nodes: int, number_of_instances_per_node: int, repetitions: int
):
    run_test_with_repetitions(
        number_of_nodes=number_of_nodes,
        number_of_instances_per_node=number_of_instances_per_node,
        repetitions=repetitions,
    )


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2_1():
    run_test_with_repetitions(2, 1, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_1_2():
    run_test_with_repetitions(1, 2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_2_2():
    run_test_with_repetitions(2, 2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_3_3():
    run_test_with_repetitions(3, 3, REPETITIONS_FOR_FUNCTIONALITY)


# identical
def run_test_with_repetitions(
    number_of_nodes: int, number_of_instances_per_node: int, repetitions: int
):
    for i in range(repetitions):
        group = f"{time.monotonic_ns()}"
        LOGGER.info(
            f"Start iteration",
            iteration=i + 1,
            repetitions=repetitions,
            group_identifier=group,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
        )
        start_time = time.monotonic()
        expected_result_of_threads, actual_result_of_threads = run_test(
            group_identifier=group,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
        )
        assert expected_result_of_threads == actual_result_of_threads
        end_time = time.monotonic()
        LOGGER.info(
            f"Finish iteration",
            iteration=i + 1,
            repetitions=repetitions,
            group_identifier=group,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
            duration=end_time - start_time,
        )


# identical
def run_test(
    group_identifier: str, number_of_nodes: int, number_of_instances_per_node: int
):
    parameters = [
        CommunicatorTestProcessParameter(
            node_name=f"n{n}",
            instance_name=f"i{i}",
            group_identifier=group_identifier,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
            local_discovery_port=Port(port=44445 + n),
            seed=0,
        )
        for n in range(number_of_nodes)
        for i in range(number_of_instances_per_node)
    ]
    processes: list[TestProcess[CommunicatorTestProcessParameter]] = [
        TestProcess(parameter, run=run) for parameter in parameters
    ]
    for process in processes:
        process.start()
    assert_processes_finish(processes, timeout_in_seconds=180)
    actual_result_of_threads: dict[tuple[str, str], str] = {}
    expected_result_of_threads: dict[tuple[str, str], str] = {}
    for process in processes:
        result_key = (process.parameter.node_name, process.parameter.instance_name)
        actual_result_of_threads[result_key] = process.get()
        expected_result_of_threads[result_key] = "Finished"
    return expected_result_of_threads, actual_result_of_threads
