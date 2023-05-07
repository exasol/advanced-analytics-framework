import time
import traceback
from pathlib import Path
from typing import Dict, Set, List

import structlog
import zmq
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.forward_register_peer_config import \
    ForwardRegisterPeerConfig
from exasol_advanced_analytics_framework.udf_communication.peer_communicator.peer_communicator_config import \
    PeerCommunicatorConfig
from exasol_advanced_analytics_framework.udf_communication.socket_factory.zmq_socket_factory import ZMQSocketFactory
from tests.integration_tests.udf_communication.peer_communicator.conditional_method_dropper import \
    ConditionalMethodDropper
from tests.integration_tests.udf_communication.peer_communicator.utils import PeerCommunicatorTestProcessParameter, \
    BidirectionalQueue, TestProcess, assert_processes_finish

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

LOGGER: FilteringBoundLogger = structlog.get_logger()


def run(parameter: PeerCommunicatorTestProcessParameter, queue: BidirectionalQueue):
    logger = LOGGER.bind(group_identifier=parameter.group_identifier, name=parameter.instance_name)
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        context = zmq.Context()
        socker_factory = ZMQSocketFactory(context)
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            socket_factory=socker_factory,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=False,
                    is_enabled=False
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
                    com.send(peer, [socker_factory.create_frame(parameter.instance_name.encode("utf8"))])
            received_values: Set[str] = set()
            for peer in com.peers():
                if peer != Peer(connection_info=com.my_connection_info):
                    value = com.recv(peer)
                    received_values.add(value[0].to_bytes().decode("utf8"))
            queue.put(received_values)
        finally:
            com.close()
            logger.info("before destroy")
            context.destroy()
            logger.info("after destroy")
    except Exception as e:
        traceback.print_exc()
        logger.exception("Exception during test", exception=e)


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_2():
    run_test_with_repetitions(2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_10():
    run_test_with_repetitions(10, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_25():
    run_test_with_repetitions(25, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_50():
    run_test_with_repetitions(50, REPETITIONS_FOR_FUNCTIONALITY)


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
    parameters = [
        PeerCommunicatorTestProcessParameter(
            instance_name=f"i{i}", group_identifier=group,
            number_of_instances=number_of_instances,
            seed=0)
        for i in range(number_of_instances)]
    processes: List[TestProcess[PeerCommunicatorTestProcessParameter]] = \
        [TestProcess(parameter, run=run) for parameter in parameters]
    for i in range(number_of_instances):
        processes[i].start()
    for i in range(number_of_instances):
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=180)
    received_values: Dict[int, Set[str]] = {}
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
