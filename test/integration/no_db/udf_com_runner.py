from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    CommunicatorTestProcessParameter,
)

import zmq
from exasol.analytics.udf.communication.communicator import Communicator
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)


LOGGER: FilteringBoundLogger = structlog.get_logger(__name__)


class CommunicatorFactory:
    def create(self, parameter: CommunicatorTestProcessParameter) -> Communicator:
        is_discovery_leader_node = parameter.node_name == "n0"
        context = zmq.Context()
        socket_factory = ZMQSocketFactory(context)
        return Communicator(
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
        

def run(parameter: CommunicatorTestProcessParameter, queue: BidirectionalQueue):
    try:
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
        value = None
        if communicator.is_multi_node_leader():
            value = b"Success"
        result = communicator.broadcast(value)
        LOGGER.info(
            "result",
            result=result,
            instance_name=parameter.instance_name,
            node_name=parameter.node_name,
        )
        queue.put(result.decode("utf-8"))
    except Exception as e:
        LOGGER.exception("Exception during test")
        queue.put(f"Failed during test: {e}")


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
        expected_result_of_threads[result_key] = "Success"
    return expected_result_of_threads, actual_result_of_threads


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
