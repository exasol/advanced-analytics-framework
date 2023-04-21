import time
from pathlib import Path
from typing import List

import pytest
import structlog
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.communicator import Communicator
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from tests.udf_communication.peer_communication.conditional_method_dropper import ConditionalMethodDropper
from tests.udf_communication.peer_communication.utils import TestProcess, BidirectionalQueue, assert_processes_finish, \
    CommunicatorTestProcessParameter

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


def run(parameter: CommunicatorTestProcessParameter,
        queue: BidirectionalQueue):
    is_discovery_leader_node = False
    if parameter.node_name == "n0":
        is_discovery_leader_node = True
    communicator = Communicator(
        global_discovery_port=Port(port=44444),
        local_discovery_port=parameter.local_discovery_port,
        global_discovery_ip=IPAddress(ip_address="127.0.0.1"),
        node_name=parameter.node_name,
        instance_name=parameter.instance_name,
        listen_ip=IPAddress(ip_address="127.0.0.1"),
        group_identifier=parameter.group_identifier,
        number_of_nodes=parameter.number_of_nodes,
        number_of_instances_per_node=parameter.number_of_instances_per_node,
        is_discovery_leader_node=is_discovery_leader_node
    )
    print("local", communicator._local_peer_communicator.peers(), flush=True)
    if not communicator._global_peer_communicator is None:
        print("global", communicator._global_peer_communicator.peers(), flush=True)


@pytest.mark.parametrize("number_of_nodes, number_of_instances_per_node, repetitions",
                         [
                             (2, 2, 100),
                             (5, 5, 10),
                         ])
def test_reliability(number_of_nodes: int, number_of_instances_per_node: int, repetitions: int):
    run_test_with_repetitions(number_of_nodes=number_of_nodes,
                              number_of_instances_per_node=number_of_instances_per_node,
                              repetitions=repetitions)


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2_2():
    run_test_with_repetitions(2, 2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_3_3():
    run_test_with_repetitions(3, 3, REPETITIONS_FOR_FUNCTIONALITY)


def run_test_with_repetitions(number_of_nodes: int, number_of_instances_per_node: int, repetitions: int):
    for i in range(repetitions):
        group = f"{time.monotonic_ns()}"
        LOGGER.info(f"Start iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    group_identifier=group,
                    number_of_nodes=number_of_nodes,
                    number_of_instances_per_node=number_of_instances_per_node)
        start_time = time.monotonic()
        run_test(group_identifier=group,
                 number_of_nodes=number_of_nodes,
                 number_of_instances_per_node=number_of_instances_per_node)
        end_time = time.monotonic()
        LOGGER.info(f"Finish iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    group_identifier=group,
                    number_of_nodes=number_of_nodes,
                    number_of_instances_per_node=number_of_instances_per_node,
                    duration=end_time - start_time)


def run_test(group_identifier: str, number_of_nodes: int, number_of_instances_per_node: int):
    parameters = [
        CommunicatorTestProcessParameter(
            node_name=f"n{n}",
            instance_name=f"i{i}",
            group_identifier=group_identifier,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
            local_discovery_port=Port(port=44445 + n),
            seed=0)
        for n in range(number_of_nodes)
        for i in range(number_of_instances_per_node)]
    processes: List[TestProcess[CommunicatorTestProcessParameter]] = \
        [TestProcess(parameter, run=run) for parameter in parameters]
    for process in processes:
        process.start()
    assert_processes_finish(processes, timeout_in_seconds=180)
