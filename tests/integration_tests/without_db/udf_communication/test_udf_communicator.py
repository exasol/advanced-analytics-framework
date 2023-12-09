import time
from pathlib import Path
from typing import List, Dict, Tuple, Union
from unittest.mock import MagicMock, create_autospec

import structlog
from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.typing import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.host_ip_addresses import HostIPAddresses
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import udf_communicator, \
    UDFCommunicatorConfig
from tests.integration_tests.without_db.udf_communication.peer_communication.conditional_method_dropper import \
    ConditionalMethodDropper
from tests.integration_tests.without_db.udf_communication.peer_communication.utils import \
    TestProcess, assert_processes_finish, BidirectionalQueue, \
    UDFCommunicatorTestProcessParameter
from tests.mock_cast import mock_cast

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(file=Path(__file__).with_suffix(".log").open("wt")),
    processors=[
        structlog.contextvars.merge_contextvars,
        ConditionalMethodDropper(method_name="debug"),
        # ConditionalMethodDropper(method_name="info"),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.processors.ExceptionRenderer(exception_formatter=ExceptionDictTransformer(locals_max_string=320)),
        structlog.processors.CallsiteParameterAdder(),
        structlog.processors.JSONRenderer()
    ]
)
LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)

MY_CONN = "my_conn"


def dummy_udf_wrapper():
    pass


def create_connection(parameter: UDFCommunicatorTestProcessParameter):
    config = UDFCommunicatorConfig(
        listen_port=Port(port=6789),
        multi_node_discovery_ip=IPAddress(ip_address="127.0.0.1", network_prefix=8),
        group_identifier_suffix="test",
        number_of_instances_per_node=parameter.number_of_instances_per_node
    )
    return Connection(address=config.json())


def run(parameter: UDFCommunicatorTestProcessParameter,
        queue: BidirectionalQueue):
    try:
        metadata = MockMetaData(
            script_code_wrapper_function=dummy_udf_wrapper,
            node_id=parameter.node_name,
            vm_id=parameter.instance_name,
            session_id=parameter.group_identifier,
            statement_id=1,
            node_count=parameter.number_of_nodes,
            input_type="SET",
            input_columns=[
                Column("test", int, "INTEGER"),
            ],
            output_type="EMITS",
            output_columns=[
                Column("test", int, "int"),
            ],
        )
        exa = MockExaEnvironment(
            metadata=metadata,
            connections={
                MY_CONN: create_connection(parameter)
            }
        )
        host_ip_addresses_mock: Union[HostIPAddresses, MagicMock] = create_autospec(HostIPAddresses)
        mock_cast(host_ip_addresses_mock.get_all_ip_addresses).return_value = [
            IPAddress(ip_address=f"127.0.0.{parameter.node_name}", network_prefix=8)]

        with udf_communicator(exa, MY_CONN, host_ip_addresses_mock):
            pass
        queue.put("Finished")
    except Exception as e:
        LOGGER.exception("Error in test")
        queue.put(f"Failed: {e}")


REPETITIONS_FOR_FUNCTIONALITY = 1


def test_functionality_1_2():
    run_test_with_repetitions(1, 2, REPETITIONS_FOR_FUNCTIONALITY)


def test_functionality_2_2():
    run_test_with_repetitions(2, 2, REPETITIONS_FOR_FUNCTIONALITY)


def run_test_with_repetitions(number_of_nodes: int, number_of_instances_per_node: int, repetitions: int):
    for i in range(repetitions):
        group = time.monotonic_ns()
        LOGGER.info(f"Start iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    group_identifier=group,
                    number_of_nodes=number_of_nodes,
                    number_of_instances_per_node=number_of_instances_per_node)
        start_time = time.monotonic()
        expected_result_of_threads, actual_result_of_threads = \
            run_test(group_identifier=group,
                     number_of_nodes=number_of_nodes,
                     number_of_instances_per_node=number_of_instances_per_node)
        assert expected_result_of_threads == actual_result_of_threads
        end_time = time.monotonic()
        LOGGER.info(f"Finish iteration",
                    iteration=i + 1,
                    repetitions=repetitions,
                    group_identifier=group,
                    number_of_nodes=number_of_nodes,
                    number_of_instances_per_node=number_of_instances_per_node,
                    duration=end_time - start_time)


def run_test(group_identifier: int, number_of_nodes: int, number_of_instances_per_node: int):
    parameters = [
        UDFCommunicatorTestProcessParameter(
            node_name=n + 1,
            instance_name=i,
            group_identifier=group_identifier,
            number_of_nodes=number_of_nodes,
            number_of_instances_per_node=number_of_instances_per_node,
            local_discovery_port=Port(port=44445 + n),
            seed=0)
        for n in range(number_of_nodes)
        for i in range(number_of_instances_per_node)]
    processes: List[TestProcess[UDFCommunicatorTestProcessParameter]] = \
        [TestProcess(parameter, run=run) for parameter in parameters]
    for process in processes:
        process.start()
    assert_processes_finish(processes, timeout_in_seconds=180)
    actual_result_of_threads: Dict[Tuple[int, int], str] = {}
    expected_result_of_threads: Dict[Tuple[int, int], str] = {}
    for process in processes:
        result_key = (process.parameter.node_name, process.parameter.instance_name)
        actual_result_of_threads[result_key] = process.get()
        expected_result_of_threads[result_key] = "Finished"
    return expected_result_of_threads, actual_result_of_threads
